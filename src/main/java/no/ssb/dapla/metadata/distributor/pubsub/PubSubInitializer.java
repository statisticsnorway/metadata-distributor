package no.ssb.dapla.metadata.distributor.pubsub;

import io.helidon.config.Config;

public class PubSubInitializer {

    public static PubSub create(Config config) {
        return new DelegatingPubSub(config);
    }
}
