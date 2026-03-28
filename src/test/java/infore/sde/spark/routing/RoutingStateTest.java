package infore.sde.spark.routing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RoutingStateTest {

    @Test
    void initialStateIsEmpty() {
        RoutingState state = new RoutingState();
        assertThat(state.getKeyedParallelism()).isEmpty();
        assertThat(state.getRegistrations()).isEmpty();
    }

    @Test
    void registerParallelism() {
        RoutingState state = new RoutingState();
        state.getKeyedParallelism().merge(2, 1, Integer::sum);
        assertThat(state.getKeyedParallelism()).containsEntry(2, 1);

        // Second registration with same parallelism increments count
        state.getKeyedParallelism().merge(2, 1, Integer::sum);
        assertThat(state.getKeyedParallelism()).containsEntry(2, 2);
    }

    @Test
    void registerAndUnregister() {
        RoutingState state = new RoutingState();
        state.getRegistrations().put(42,
                new RoutingState.RoutingRegistration(1, 2, "Forex"));
        assertThat(state.getRegistrations()).containsKey(42);

        RoutingState.RoutingRegistration removed = state.getRegistrations().remove(42);
        assertThat(removed.getNoOfP()).isEqualTo(2);
        assertThat(removed.getDataSetKey()).isEqualTo("Forex");
        assertThat(state.getRegistrations()).isEmpty();
    }
}
