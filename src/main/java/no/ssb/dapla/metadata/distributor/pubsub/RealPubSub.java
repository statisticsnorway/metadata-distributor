package no.ssb.dapla.metadata.distributor.pubsub;

import com.google.api.gax.core.CredentialsProvider;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class RealPubSub implements PubSub {

    public static RealPubSub createWithServiceAccountKeyCredentials(String serviceAccountKeyPath) {
        Path serviceAccountKeyFilePath = Path.of(serviceAccountKeyPath);
        GoogleCredentials credentials;
        try {
            credentials = ServiceAccountCredentials.fromStream(
                    Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new RealPubSub(() -> credentials);
    }

    public static RealPubSub createWithComputeEngineCredentials() {
        GoogleCredentials credentials = ComputeEngineCredentials.create();
        return new RealPubSub(() -> credentials);
    }

    public static RealPubSub createWithDefaultCredentials() {
        GoogleCredentials credentials = null;
        return new RealPubSub(() -> credentials);
    }

    private static final Logger LOG = LoggerFactory.getLogger(RealPubSub.class);

    final CredentialsProvider credentialsProvider;

    public RealPubSub(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
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