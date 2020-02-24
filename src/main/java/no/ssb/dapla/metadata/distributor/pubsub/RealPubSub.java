package no.ssb.dapla.metadata.distributor.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import io.helidon.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class RealPubSub implements PubSub {

    private static final Logger LOG = LoggerFactory.getLogger(RealPubSub.class);

    private static Credentials getCredentials(Config config) {
        String configuredProviderChoice = config.get("credential-provider").asString().orElse("default");
        if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Running with the service-account google bigtable credentials provider");
            Path serviceAccountKeyFilePath = Path.of(config.get("credentials.service-account.path").asString()
                    .orElseThrow(() -> new RuntimeException("'credentials.service-account.path' missing from bigtable config"))
            );
            GoogleCredentials credentials;
            try {
                credentials = ServiceAccountCredentials.fromStream(
                        Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return credentials;
        } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Running with the compute-engine google bigtable credentials provider");
            return ComputeEngineCredentials.create();
        } else { // default
            LOG.info("Running with the default google bigtable credentials provider");
            return null;
        }
    }

    final CredentialsProvider credentialsProvider;

    RealPubSub(Config config) {
        Credentials credentials = getCredentials(config);
        credentialsProvider = () -> credentials;
    }

    @Override
    public TopicAdminClient getTopicAdminClient() {
        try {
            return TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SubscriptionAdminClient getSubscriptionAdminClient() {
        try {
            return SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Publisher getPublisher(ProjectTopicName projectTopicName) {
        try {
            return Publisher.newBuilder(projectTopicName)
                    .setCredentialsProvider(credentialsProvider)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Subscriber getSubscriber(ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
        return Subscriber.newBuilder(projectSubscriptionName, messageReceiver)
                .setCredentialsProvider(credentialsProvider)
                .build();
    }
}