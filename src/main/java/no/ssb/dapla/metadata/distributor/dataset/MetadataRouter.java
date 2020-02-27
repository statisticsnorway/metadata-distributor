package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.helidon.config.Config;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
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

public class MetadataRouter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRouter.class);

    final PubSub pubSub;
    final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    final List<Publisher> publishers = new CopyOnWriteArrayList<>();

    public MetadataRouter(Config routeConfig, PubSub pubSub) {
        this.pubSub = pubSub;

        List<Config> upstreams = routeConfig.get("upstream").asNodeList().get();
        List<Config> downstreams = routeConfig.get("downstream").asNodeList().get();

        try (TopicAdminClient topicAdminClient = pubSub.getTopicAdminClient()) {
            for (Config downstream : downstreams) {
                // ensure that topics exists
                String downstreamProjectId = downstream.get("projectId").asString().get();
                String downstreamTopic = downstream.get("topic").asString().get();
                ProjectTopicName downstreamProjectTopicName = ProjectTopicName.of(downstreamProjectId, downstreamTopic);
                if (!pubSub.topicExists(topicAdminClient, ProjectName.of(downstreamProjectId), downstreamProjectTopicName, 25)) {
                    topicAdminClient.createTopic(downstreamProjectTopicName);
                }
                // create downstream publisher
                Publisher publisher = pubSub.getPublisher(downstreamProjectTopicName);
                publishers.add(publisher);
            }

            try (SubscriptionAdminClient subscriptionAdminClient = pubSub.getSubscriptionAdminClient()) {
                for (Config upstream : upstreams) {
                    String upstreamProjectId = upstream.get("projectId").asString().get();
                    ProjectName upstreamProjectName = ProjectName.of(upstreamProjectId);
                    String upstreamTopicName = upstream.get("topic").asString().get();
                    String upstreamSubscriptionName = upstream.get("subscription").asString().get();
                    ProjectTopicName upstreamProjectTopicName = ProjectTopicName.of(upstreamProjectId, upstreamTopicName);
                    ProjectSubscriptionName upstreamProjectSubscriptionName = ProjectSubscriptionName.of(upstreamProjectId, upstreamSubscriptionName);

                    if (!pubSub.topicExists(topicAdminClient, upstreamProjectName, upstreamProjectTopicName, 25)) {
                        topicAdminClient.createTopic(upstreamProjectTopicName);
                    }
                    if (!pubSub.subscriptionExists(subscriptionAdminClient, upstreamProjectName, upstreamProjectSubscriptionName, 25)) {
                        subscriptionAdminClient.createSubscription(upstreamProjectSubscriptionName.toString(), upstreamProjectTopicName.toString(), PushConfig.getDefaultInstance(), 10);
                    }
                    MessageReceiver messageReceiver = new DataChangedReceiver(publishers, upstreamProjectTopicName, upstreamProjectSubscriptionName);
                    Subscriber subscriber = pubSub.getSubscriber(upstreamProjectSubscriptionName, messageReceiver);
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
                return this;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return future;
    }

    static DatasetMeta resolveAndReadDatasetMeta(DataChangedRequest request) throws IOException {
        DatasetMeta datasetMeta;
        DatasetUri datasetUri = DatasetUri.of(request.getParentUri(), request.getPath(), request.getVersion());
        String scheme = datasetUri.toURI().getScheme();
        switch (scheme) {
            case "file":
                Path pathToDatasetMetaJson = Path.of(
                        datasetUri.getPathPrefix() +
                                datasetUri.getPath()
                                + "/" + request.getVersion()
                                + "/" + request.getFilename()
                );
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
