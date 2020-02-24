package no.ssb.dapla.metadata.distributor;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
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
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.helidon.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;
import java.util.function.Supplier;

public class GooglePubSubInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(GooglePubSubInitializer.class);

    private static GoogleCredentials getCredentials(Config googleConfig) {
        String configuredProviderChoice = googleConfig.get("credential-provider").asString().orElse("default");
        if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Running with the service-account google bigtable credentials provider");
            Path serviceAccountKeyFilePath = Path.of(googleConfig.get("credentials.service-account.path").asString()
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

    public static class SubscriberConfig {
        private final ProjectTopicName projectTopicName;
        private final ProjectSubscriptionName projectSubscriptionName;
        private final MessageReceiver messageReceiver;

        public SubscriberConfig(ProjectTopicName projectTopicName, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
            this.projectTopicName = projectTopicName;
            this.projectSubscriptionName = projectSubscriptionName;
            this.messageReceiver = messageReceiver;
        }

        public ProjectTopicName getProjectTopicName() {
            return projectTopicName;
        }

        public ProjectSubscriptionName getProjectSubscriptionName() {
            return projectSubscriptionName;
        }

        public MessageReceiver getMessageReceiver() {
            return messageReceiver;
        }
    }

    private Function<ProjectTopicName, Publisher> publisherFactory;
    private Function<SubscriberConfig, Subscriber> subscriberFactory;
    private Supplier<TopicAdminClient> topicAdminClientSupplier;
    private Supplier<SubscriptionAdminClient> subscriptionAdminClientSupplier;

    GooglePubSubInitializer(Config pubsubConfig) {
        boolean useEmulator = pubsubConfig.get("use-emulator").asBoolean().orElse(false);
        if (useEmulator) {
            Config emulatorConfig = pubsubConfig.get("emulator");
            String host = emulatorConfig.get("host").asString().get();
            int port = emulatorConfig.get("port").asInt().get();

            ManagedChannel pubSubChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

            FixedTransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(pubSubChannel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

            topicAdminClientSupplier = () -> {
                try {
                    return TopicAdminClient.create(
                            TopicAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(credentialsProvider)
                                    .build());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            subscriptionAdminClientSupplier = () -> {
                try {
                    return SubscriptionAdminClient.create(
                            SubscriptionAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(credentialsProvider)
                                    .build());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            publisherFactory = ptn -> {
                try {
                    return Publisher.newBuilder(ptn)
                            .setChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            subscriberFactory = subscriberConfig -> {
                try (SubscriptionAdminClient subscriptionAdminClient = subscriptionAdminClientSupplier.get()) {
                    if (!subscriptionExists(subscriptionAdminClient, ProjectName.of(subscriberConfig.getProjectTopicName().getProject()), subscriberConfig.getProjectSubscriptionName())) {
                        subscriptionAdminClient.createSubscription(subscriberConfig.getProjectSubscriptionName(), subscriberConfig.getProjectTopicName(), PushConfig.getDefaultInstance(), 10);
                    }
                    return Subscriber.newBuilder(subscriberConfig.getProjectSubscriptionName(), subscriberConfig.getMessageReceiver())
                            .setChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build();
                }
            };
        } else {
            // TODO implement real google connection
        }
    }

    public Function<ProjectTopicName, Publisher> getPublisherFactory() {
        return publisherFactory;
    }

    public Function<SubscriberConfig, Subscriber> getSubscriberFactory() {
        return subscriberFactory;
    }

    public Supplier<TopicAdminClient> getTopicAdminClientSupplier() {
        return topicAdminClientSupplier;
    }

    public Supplier<SubscriptionAdminClient> getSubscriptionAdminClientSupplier() {
        return subscriptionAdminClientSupplier;
    }

    public boolean topicExists(TopicAdminClient topicAdminClient, ProjectName projectName, ProjectTopicName projectTopicName) {
        final int PAGE_SIZE = 25;
        TopicAdminClient.ListTopicsPagedResponse listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Topic topic : listResponse.iterateAll()) {
            if (topic.getName().equals(projectTopicName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Topic topic : listResponse.iterateAll()) {
                if (topic.getName().equals(projectTopicName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean subscriptionExists(SubscriptionAdminClient subscriptionAdminClient, ProjectName projectName, ProjectSubscriptionName projectSubscriptionName) {
        final int PAGE_SIZE = 25;
        SubscriptionAdminClient.ListSubscriptionsPagedResponse listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Subscription subscription : listResponse.iterateAll()) {
            if (subscription.getName().equals(projectSubscriptionName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Subscription subscription : listResponse.iterateAll()) {
                if (subscription.getName().equals(projectSubscriptionName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}
