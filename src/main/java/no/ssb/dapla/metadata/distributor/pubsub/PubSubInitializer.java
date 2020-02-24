package no.ssb.dapla.metadata.distributor.pubsub;

import io.helidon.config.Config;

public class PubSubInitializer {

    public static PubSub create(Config config) {
        boolean useEmulator = config.get("use-emulator").asBoolean().orElse(false);
        if (useEmulator) {
            Config emulatorConfig = config.get("emulator");
            return new EmulatorPubSub(
                    emulatorConfig.get("host").asString().get(),
                    emulatorConfig.get("port").asInt().get()
            );
        } else {
            return new RealPubSub(config);
        }
    }
}
