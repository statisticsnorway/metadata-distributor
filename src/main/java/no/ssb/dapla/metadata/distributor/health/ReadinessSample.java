package no.ssb.dapla.metadata.distributor.health;

public class ReadinessSample {
    final boolean connected;
    final long time;

    public ReadinessSample(boolean connected, long time) {
        this.connected = connected;
        this.time = time;
    }
}
