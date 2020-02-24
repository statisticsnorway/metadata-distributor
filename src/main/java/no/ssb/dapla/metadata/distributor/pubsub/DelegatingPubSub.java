package no.ssb.dapla.metadata.distributor.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import io.helidon.config.Config;

class DelegatingPubSub implements PubSub {

    final PubSub delegate;

    DelegatingPubSub(Config config) {
        boolean useEmulator = config.get("use-emulator").asBoolean().orElse(false);
        if (useEmulator) {
            Config emulatorConfig = config.get("emulator");
            delegate = new EmulatorPubSub(
                    emulatorConfig.get("host").asString().get(),
                    emulatorConfig.get("port").asInt().get()
            );
        } else {
            delegate = new RealPubSub(config);
        }
    }

    @Override
    public TopicAdminClient getTopicAdminClient() {
        return delegate.getTopicAdminClient();
    }

    @Override
    public SubscriptionAdminClient getSubscriptionAdminClient() {
        return delegate.getSubscriptionAdminClient();
    }

    @Override
    public Publisher getPublisher(ProjectTopicName projectTopicName) {
        return delegate.getPublisher(projectTopicName);
    }

    @Override
    public Subscriber getSubscriber(ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
        return delegate.getSubscriber(projectSubscriptionName, messageReceiver);
    }

}
