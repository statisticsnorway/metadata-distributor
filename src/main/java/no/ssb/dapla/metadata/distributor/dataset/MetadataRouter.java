package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.helidon.config.Config;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataRouter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRouter.class);

    final TransportChannelProvider channelProvider;
    final CredentialsProvider credentialsProvider;

    final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    final List<Publisher> publishers = new CopyOnWriteArrayList<>();

    public MetadataRouter(Config routeConfig, TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider) {
        try {
            this.channelProvider = channelProvider;
            this.credentialsProvider = credentialsProvider;

            List<Config> upstreams = routeConfig.get("upstream").asNodeList().get();
            List<Config> downstreams = routeConfig.get("downstream").asNodeList().get();

            for (Config downstream : downstreams) {
                // ensure that topics exists
                String downstreamProjectId = downstream.get("projectId").asString().get();
                String downstreamTopic = downstream.get("topic").asString().get();
                ProjectTopicName downstreamProjectTopicName = ProjectTopicName.of(downstreamProjectId, downstreamTopic);
                try (TopicAdminClient topicAdminClient = getTopicAdminClient()) {
                    if (!topicExists(topicAdminClient, ProjectName.of(downstreamProjectId), downstreamProjectTopicName)) {
                        topicAdminClient.createTopic(downstreamProjectTopicName);
                    }
                }
                // create downstream publisher
                Publisher publisher = Publisher.newBuilder(downstreamProjectTopicName)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();
                publishers.add(publisher);
            }

            for (Config upstream : upstreams) {
                String upstreamProjectId = upstream.get("projectId").asString().get();
                ProjectName upstreamProjectName = ProjectName.of(upstreamProjectId);
                String upstreamTopicName = upstream.get("topic").asString().get();
                String upstreamSubscriptionName = upstream.get("subscription").asString().get();
                ProjectTopicName upstreamProjectTopicName = ProjectTopicName.of(upstreamProjectId, upstreamTopicName);
                ProjectSubscriptionName upstreamProjectSubscriptionName = ProjectSubscriptionName.of(upstreamProjectId, upstreamSubscriptionName);

                try (TopicAdminClient topicAdminClient = getTopicAdminClient()) {
                    if (!topicExists(topicAdminClient, upstreamProjectName, upstreamProjectTopicName)) {
                        topicAdminClient.createTopic(upstreamProjectTopicName);
                    }
                    try (SubscriptionAdminClient subscriptionAdminClient = getSubscriptionAdminClient()) {
                        if (!subscriptionExists(subscriptionAdminClient, upstreamProjectName, upstreamProjectSubscriptionName)) {
                            subscriptionAdminClient.createSubscription(upstreamProjectSubscriptionName, upstreamProjectTopicName, PushConfig.getDefaultInstance(), 10);
                        }
                        MessageReceiver messageReceiver = new DataChangedReceiver(publishers, upstreamProjectTopicName, upstreamProjectSubscriptionName);
                        Subscriber subscriber = Subscriber.newBuilder(upstreamProjectSubscriptionName, messageReceiver)
                                .setChannelProvider(this.channelProvider)
                                .setCredentialsProvider(this.credentialsProvider)
                                .build();
                        subscriber.addListener(
                                new Subscriber.Listener() {
                                    public void failed(Subscriber.State from, Throwable failure) {
                                        LOG.error(String.format("Error with subscriber on subscription: '%s'", upstreamProjectSubscriptionName), failure);
                                    }
                                },
                                MoreExecutors.directExecutor());
                        subscriber.startAsync().awaitRunning();
                        subscribers.add(subscriber);
                    }
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean topicExists(TopicAdminClient topicAdminClient, ProjectName projectName, ProjectTopicName projectTopicName) {
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

    private boolean subscriptionExists(SubscriptionAdminClient subscriptionAdminClient, ProjectName projectName, ProjectSubscriptionName projectSubscriptionName) {
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

    private TopicAdminClient getTopicAdminClient() {
        try {
            return TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SubscriptionAdminClient getSubscriptionAdminClient() {
        try {
            return SubscriptionAdminClient.create(
                    SubscriptionAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<MetadataRouter> shutdown() {
        CompletableFuture<MetadataRouter> future = CompletableFuture.supplyAsync(() -> {
            try {
                for (Subscriber subscriber : subscribers) {
                    subscriber.stopAsync();
                }
                for (Subscriber subscriber : subscribers) {
                    subscriber.awaitTerminated();
                }
                for (Publisher publisher : publishers) {
                    publisher.shutdown();
                }
                for (Publisher publisher : publishers) {
                    while (!publisher.awaitTermination(3, TimeUnit.SECONDS)) {
                    }
                }
                for (Publisher publisher : publishers) {
                    publisher.shutdown();
                }
                return this;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return future;
    }

    static DatasetMeta resolveAndReadDatasetMeta(DataChangedRequest request) throws IOException {
        DatasetMeta datasetMeta;
        String parentUri = request.getParentUri();
        Pattern parentUriPattern = Pattern.compile("(?<scheme>[^:]+):(?://(?<host>[^/]*))?(?<path>/[^/].*)");
        Matcher m = parentUriPattern.matcher(parentUri);
        if (!m.matches()) {
            throw new RuntimeException("Invalid parentUri, does not conform to URI pattern: " + parentUriPattern.pattern());
        }
        String scheme = m.group("scheme");
        switch (scheme) {
            case "file":
                Path pathToDatasetMetaJson = Path.of(m.group("path"), request.getPath() + "/" + request.getVersion() + "/dataset-meta.json");
                String datasetMetaJson = Files.readString(pathToDatasetMetaJson, StandardCharsets.UTF_8);
                datasetMeta = ProtobufJsonUtils.toPojo(datasetMetaJson, DatasetMeta.class);
                break;
            case "gs":
                throw new RuntimeException("Fetch from Google Cloud Storage not yet implemented"); // TODO
            default:
                throw new RuntimeException("Scheme not supported. scheme='" + scheme + "'");
        }
        return datasetMeta;
    }

    static class DataChangedReceiver implements MessageReceiver {
        private static final Logger LOG = LoggerFactory.getLogger(DataChangedReceiver.class);

        final List<Publisher> publishers;
        final ProjectTopicName projectTopicName;
        final ProjectSubscriptionName projectSubscriptionName;

        DataChangedReceiver(List<Publisher> publishers, ProjectTopicName projectTopicName, ProjectSubscriptionName projectSubscriptionName) {
            this.publishers = publishers;
            this.projectTopicName = projectTopicName;
            this.projectSubscriptionName = projectSubscriptionName;
        }

        @Override
        public void receiveMessage(PubsubMessage upstreamMessage, AckReplyConsumer consumer) {
            try {
                DataChangedRequest request = DataChangedRequest.parseFrom(upstreamMessage.getData());
                String upstreamJson = ProtobufJsonUtils.toString(request);
                LOG.debug(
                        String.format("receiveMessage()%n  topic:        '%s'%n  subscription: '%s'%n  payload:%n%s%n",
                                projectTopicName.toString(),
                                projectSubscriptionName.toString(),
                                upstreamJson
                        )
                );
                AtomicInteger succeeded = new AtomicInteger(0);

                DatasetMeta datasetMeta = resolveAndReadDatasetMeta(request);
                ByteString datasetMetaByteString = datasetMeta.toByteString();

                for (Publisher publisher : publishers) {
                    PubsubMessage downstreamMessage = PubsubMessage.newBuilder().setData(datasetMetaByteString).build();
                    ApiFuture<String> publishResponseFuture = publisher.publish(downstreamMessage);
                    ApiFutures.addCallback(publishResponseFuture, new ApiFutureCallback<>() {
                                @Override
                                public void onSuccess(String result) {
                                    if (succeeded.incrementAndGet() == publishers.size()) {
                                        consumer.ack();
                                    }
                                }

                                @Override
                                public void onFailure(Throwable throwable) {
                                    try {
                                        String upstreamJson = ProtobufJsonUtils.toString(DataChangedRequest.parseFrom(upstreamMessage.getData()));
                                        LOG.error(
                                                String.format("Failed to publish message downstream. Sending nack upstream to force re-delivery.%s%n  downstream-topic: %s%n  upstream-topic: '%s'%n  upstream-subscription: '%s'%n  upstream-payload:%n%s%n",
                                                        publisher.getTopicName(),
                                                        projectTopicName,
                                                        projectSubscriptionName,
                                                        upstreamJson
                                                ),
                                                throwable
                                        );
                                    } catch (InvalidProtocolBufferException e) {
                                        throw new RuntimeException(e);
                                    } finally {
                                        consumer.nack(); // force re-delivery
                                    }
                                }
                            },
                            MoreExecutors.directExecutor()
                    );
                }
            } catch (RuntimeException | Error e) {
                LOG.error("Sending nack on message to force re-delivery", e);
                consumer.nack();
                throw e;
            } catch (IOException e) {
                LOG.error("Sending nack on message to force re-delivery", e);
                consumer.nack();
                throw new RuntimeException(e);
            }
        }
    }
}
