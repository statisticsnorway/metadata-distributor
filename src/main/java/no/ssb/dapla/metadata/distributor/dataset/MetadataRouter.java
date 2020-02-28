package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
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
    final Storage storage;
    final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    final List<Publisher> publishers = new CopyOnWriteArrayList<>();

    public MetadataRouter(Config routeConfig, PubSub pubSub, Storage storage) {
        this.pubSub = pubSub;
        this.storage = storage;

        List<Config> upstreams = routeConfig.get("upstream").asNodeList().get();
        List<Config> downstreams = routeConfig.get("downstream").asNodeList().get();

        for (Config downstream : downstreams) {
            String downstreamProjectId = downstream.get("projectId").asString().get();
            String downstreamTopic = downstream.get("topic").asString().get();
            LOG.info("Creating publisher on topic: {}", downstreamTopic);
            Publisher publisher = pubSub.getPublisher(downstreamProjectId, downstreamTopic);
            publishers.add(publisher);
        }

        for (Config upstream : upstreams) {
            String upstreamProjectId = upstream.get("projectId").asString().get();
            String upstreamTopicName = upstream.get("topic").asString().get();
            String upstreamSubscriptionName = upstream.get("subscription").asString().get();

            MessageReceiver messageReceiver = new DataChangedReceiver(upstreamProjectId, upstreamTopicName, upstreamSubscriptionName);
            LOG.info("Creating subscriber on subscription: {}", upstreamSubscriptionName);
            Subscriber subscriber = pubSub.getSubscriber(upstreamProjectId, upstreamSubscriptionName, messageReceiver);
            subscriber.addListener(
                    new Subscriber.Listener() {
                        public void failed(Subscriber.State from, Throwable failure) {
                            LOG.error(String.format("Error with subscriber on subscription: '%s'", upstreamSubscriptionName), failure);
                        }
                    },
                    MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();
            LOG.info("Subscriber is ready to receive messages: {}", upstreamSubscriptionName);
            subscribers.add(subscriber);
        }
    }

    DatasetMeta resolveAndReadDatasetMeta(DataChangedRequest request) throws IOException {
        String datasetMetaJson;
        DatasetUri datasetUri = DatasetUri.of(request.getParentUri(), request.getPath(), request.getVersion());
        String localPath = datasetUri.toURI().getPath() + "/" + request.getFilename();
        String scheme = datasetUri.toURI().getScheme();
        switch (scheme) {
            case "file":
                Path pathToDatasetMetaJson = Path.of(localPath);
                datasetMetaJson = Files.readString(pathToDatasetMetaJson, StandardCharsets.UTF_8);
                break;
            case "gs":
                String bucket = datasetUri.toURI().getHost();
                byte[] datasetMetaBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(localPath)));
                datasetMetaJson = new String(datasetMetaBytes, StandardCharsets.UTF_8);
                break;
            default:
                throw new RuntimeException("Scheme not supported. scheme='" + scheme + "'");
        }
        DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(datasetMetaJson, DatasetMeta.class);
        return datasetMeta;
    }

    private static String stripLeadingSlashes(String input) {
        return input.startsWith("/") ? stripLeadingSlashes(input.substring(1)) : input;
    }

    class DataChangedReceiver implements MessageReceiver {

        final String projectId;
        final String topic;
        final String subscription;

        DataChangedReceiver(String projectId, String topic, String subscription) {
            this.projectId = projectId;
            this.topic = topic;
            this.subscription = subscription;
        }

        @Override
        public void receiveMessage(PubsubMessage upstreamMessage, AckReplyConsumer consumer) {
            try {
                DataChangedRequest request = DataChangedRequest.parseFrom(upstreamMessage.getData());
                AtomicInteger succeeded = new AtomicInteger(0);

                String upstreamJson = ProtobufJsonUtils.toString(request);

                if (!".dataset-meta.json".equals(request.getFilename())) {
                    consumer.ack();
                    LOG.debug("Ignored DataChangedRequest message: {}", upstreamJson);
                    return;
                }

                LOG.debug(String.format("processing DataChangedRequest message%n  topic:        '%s'%n  subscription: '%s'%n  payload:%n%s%n", topic, subscription, upstreamJson));

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
                                    LOG.error(
                                            String.format("Failed to publish message downstream. Upstream subscription must wait for deadline before message is eligible for re-delivery.%s%n  downstream-topic: %s%n  upstream-topic: '%s'%n  upstream-subscription: '%s'%n  upstream-payload:%n%s%n",
                                                    publisher.getTopicName(),
                                                    topic,
                                                    subscription,
                                                    upstreamJson
                                            ),
                                            throwable
                                    );
                                }
                            },
                            MoreExecutors.directExecutor()
                    );
                }
            } catch (RuntimeException | Error e) {
                LOG.error("Error while processing message, upstream subscription must wait for deadline before message is eligible for re-delivery", e);
            } catch (IOException e) {
                LOG.error("Error while processing message, upstream subscription must wait for deadline before message is eligible for re-delivery", e);
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
}
