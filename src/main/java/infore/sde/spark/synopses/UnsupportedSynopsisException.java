package infore.sde.spark.synopses;

public class UnsupportedSynopsisException extends Exception {

    private final int synopsisID;

    public UnsupportedSynopsisException(int synopsisID) {
        super("Unsupported synopsisID " + synopsisID + ". Supported: [1, 2, 3, 4]");
        this.synopsisID = synopsisID;
    }

    public int getSynopsisID() { return synopsisID; }
}
