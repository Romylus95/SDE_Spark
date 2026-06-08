package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.util.concurrent.{Executors, ThreadFactory}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}

/**
 * Patched KafkaOffsetReader for SoftNet HDP 3.1.0 cluster.
 *
 * Root cause: consumer.poll(0) in assign mode unconditionally calls
 * refreshCommittedOffsetsIfNeeded() → fetchCommittedOffsets() → ensureCoordinatorReady()
 * → awaitMetadataUpdate(Long.MAX_VALUE). On this cluster, ~21/50 __consumer_offsets
 * partitions have Leader: -1 (dead broker replicas never cleaned up). When the random
 * UUID group.id hashes to one of those partitions, FindCoordinator returns
 * COORDINATOR_NOT_AVAILABLE and the driver hangs forever.
 *
 * Fix: replace poll(0) + seekToBeginning/End + position() with beginningOffsets() /
 * endOffsets(), which send ListOffsets requests directly to partition leaders —
 * no coordinator contact at all.
 */
private[kafka010] class KafkaOffsetReader(
    consumerStrategy: ConsumerStrategy,
    val driverKafkaParams: ju.Map[String, Object],
    readerOptions: Map[String, String],
    driverGroupIdPrefix: String) extends Logging {

  private val kafkaReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("kafka-get-offsets") {
        override def run(): Unit = r.run()
      }
      t.setDaemon(true)
      t
    }
  })

  private val execContext = ExecutionContext.fromExecutorService(kafkaReaderThread)

  // Initialization order must match original: groupId/nextId before consumer,
  // maxOffsetFetchAttempts after consumer creation.
  private var groupId: String = _
  private var nextId = 0
  private var consumer: Consumer[Array[Byte], Array[Byte]] = createConsumer()

  private[kafka010] val maxOffsetFetchAttempts =
    readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private[kafka010] val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  override def toString: String = s"KafkaReader for $consumerStrategy"

  def close(): Unit = {
    runUninterruptibly { consumer.close() }
    kafkaReaderThread.shutdown()
  }

  def fetchTopicPartitions(): Set[TopicPartition] = runUninterruptibly {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    // PATCH: removed poll(0) — in assign mode, assignment() already has all partitions.
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    partitions.asScala.toSet
  }

  def fetchEarliestOffsets(): Map[TopicPartition, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      // PATCH: beginningOffsets() sends ListOffsets to partition leaders — no coordinator.
      val partitions = consumer.assignment().asScala
      consumer.pause(partitions.asJava)
      logDebug(s"Fetching earliest offsets for ${partitions.size} partitions")
      val result = consumer.beginningOffsets(partitions.asJava).asScala
        .map { case (tp, off) => tp -> off.toLong }
        .toMap
      logDebug(s"Got earliest offsets for ${result.size} partitions: ${result.mkString(",")}")
      result
    }
  }

  def fetchLatestOffsets(): Map[TopicPartition, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      // PATCH: endOffsets() sends ListOffsets to partition leaders — no coordinator.
      val partitions = consumer.assignment().asScala
      consumer.pause(partitions.asJava)
      logDebug(s"Fetching latest offsets for ${partitions.size} partitions")
      val result = consumer.endOffsets(partitions.asJava).asScala
        .map { case (tp, off) => tp -> off.toLong }
        .toMap
      logDebug(s"Got latest offsets for ${result.size} partitions: ${result.mkString(",")}")
      result
    }
  }

  def fetchEarliestOffsets(newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      runUninterruptibly {
        withRetriesWithoutInterrupt {
          // PATCH: beginningOffsets() — no coordinator contact.
          val allAssigned = consumer.assignment()
          consumer.pause(allAssigned)
          logDebug(s"Fetching earliest offsets for new partitions: $newPartitions")
          val valid = newPartitions.filter(allAssigned.contains)
          val result = consumer.beginningOffsets(valid.asJava).asScala
            .map { case (tp, off) => tp -> off.toLong }
            .toMap
          logDebug(s"Got earliest offsets for ${result.size} new partitions: ${result.mkString(",")}")
          result
        }
      }
    }
  }

  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset = {
    val fetched = runUninterruptibly {
      withRetriesWithoutInterrupt {
        // PATCH: removed poll(0). seek* puts partitions into awaitingReset state so
        // position() uses ListOffsets to partition leaders, not the coordinator.
        val partitions = consumer.assignment().asScala
        consumer.pause(partitions.asJava)
        assert(partitionOffsets.keySet == partitions.toSet,
          s"Some partitions missing from consumer assignment: " +
            s"${partitionOffsets.keySet -- partitions}")
        logDebug(s"Seeking to specific offsets: $partitionOffsets")
        partitionOffsets.foreach {
          case (tp, off) if off == KafkaOffsetRangeLimit.LATEST =>
            consumer.seekToEnd(Seq(tp).asJava)
          case (tp, off) if off == KafkaOffsetRangeLimit.EARLIEST =>
            consumer.seekToBeginning(Seq(tp).asJava)
          case (tp, off) =>
            consumer.seek(tp, off)
        }
        partitions.map(tp => tp -> consumer.position(tp)).toMap
      }
    }
    partitionOffsets.foreach { case (tp, requestedOffset) =>
      if (requestedOffset >= 0 && fetched(tp) != requestedOffset) {
        reportDataLoss(
          s"Partition $tp: requested offset $requestedOffset unavailable, " +
            s"skipping to ${fetched(tp)}")
      }
    }
    KafkaSourceOffset(fetched)
  }

  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread().isInstanceOf[UninterruptibleThread]) {
      val future = scala.concurrent.Future(body)(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  private[kafka010] def withRetriesWithoutInterrupt(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
          && !Thread.currentThread().isInterrupted) {
        Thread.currentThread() match {
          case ut: UninterruptibleThread =>
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case e: OffsetOutOfRangeException => throw e
                case NonFatal(e) =>
                  lastException = e
                  logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  resetConsumer()
              }
            }
          case _ =>
            throw new IllegalStateException(
              "withRetriesWithoutInterrupt must be called from UninterruptibleThread")
        }
      }
      if (Thread.currentThread().isInterrupted) throw new InterruptedException()
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }

  private def createConsumer(): Consumer[Array[Byte], Array[Byte]] = synchronized {
    val params = new ju.HashMap[String, Object](driverKafkaParams)
    params.put(ConsumerConfig.GROUP_ID_CONFIG, nextGroupId())
    consumerStrategy.createConsumer(params)
  }

  private[kafka010] def resetConsumer(): Unit = synchronized {
    consumer.close()
    consumer = createConsumer()
  }

  private def nextGroupId(): String = {
    groupId = s"$driverGroupIdPrefix-$nextId"
    nextId += 1
    groupId
  }
}

object KafkaOffsetReader {
  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)))
}
