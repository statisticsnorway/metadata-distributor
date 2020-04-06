package no.ssb.dapla.metadata.distributor.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataRouter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRouter.class);

    final PubSub pubSub;
    final Storage storage;
    final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    final List<Publisher> publishers = new CopyOnWriteArrayList<>();
    final MetadataSignatureVerifier metadataSignatureVerifier;

    public MetadataRouter(Config routeConfig, PubSub pubSub, Storage storage, MetadataSignatureVerifier metadataSignatureVerifier) {
        this.pubSub = pubSub;
        this.storage = storage;
        this.metadataSignatureVerifier = metadataSignatureVerifier;

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
            boolean subscribe = upstream.get("subscribe").asBoolean().orElse(true);
            String upstreamProjectId = upstream.get("projectId").asString().get();
            String upstreamTopicName = upstream.get("topic").asString().get();
            String upstreamSubscriptionName = upstream.get("subscription").asString().get();

            if (subscribe) {
                MessageReceiver messageReceiver = new RouterMessageReceiver(upstreamProjectId, upstreamTopicName, upstreamSubscriptionName);
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
            } else {
                LOG.info("SUBSCRIPTION DISABLED: {}", upstreamSubscriptionName);
            }
        }
    }

    static MetadataReadAndVerifyResult resolveAndReadDatasetMeta(
            Storage storage,
            MetadataSignatureVerifier metadataSignatureVerifier,
            DatasetUri datasetUri
    ) throws IOException {
        byte[] datasetMetaBytes;
        byte[] datasetMetaSignatureBytes;
        String datasetMetaJsonPath = datasetUri.toURI().getPath() + "/.dataset-meta.json";
        String datasetMetaJsonSignaturePath = datasetUri.toURI().getPath() + "/.dataset-meta.json.sign";
        String scheme = datasetUri.toURI().getScheme();
        switch (scheme) {
            case "file":
                datasetMetaBytes = Files.readAllBytes(Path.of(datasetMetaJsonPath));
                datasetMetaSignatureBytes = Files.readAllBytes(Path.of(datasetMetaJsonSignaturePath));
                break;
            case "gs":
                String bucket = datasetUri.toURI().getHost();
                datasetMetaBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonPath)));
                datasetMetaSignatureBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonSignaturePath)));
                break;
            default:
                throw new RuntimeException("Scheme not supported. scheme='" + scheme + "'");
        }
        boolean verified = metadataSignatureVerifier.verify(datasetMetaBytes, datasetMetaSignatureBytes);
        return new MetadataReadAndVerifyResult(verified, ByteString.copyFrom(datasetMetaBytes));
    }

    private static String stripLeadingSlashes(String input) {
        return input.startsWith("/") ? stripLeadingSlashes(input.substring(1)) : input;
    }

    static class MetadataReadAndVerifyResult {

        final boolean signatureValid;
        final ByteString datasetMetaByteString;

        MetadataReadAndVerifyResult(boolean signatureValid, ByteString datasetMetaByteString) {
            this.signatureValid = signatureValid;
            this.datasetMetaByteString = datasetMetaByteString;
        }

    }

    static final ObjectMapper objectMapper = new ObjectMapper();

    class RouterMessageReceiver implements MessageReceiver {

        final String projectId;
        final String topic;
        final String subscription;

        RouterMessageReceiver(String projectId, String topic, String subscription) {
            this.projectId = projectId;
            this.topic = topic;
            this.subscription = subscription;
        }

        @Override
        public void receiveMessage(PubsubMessage upstreamMessage, AckReplyConsumer consumer) {
            process(storage, metadataSignatureVerifier, publishers, topic, subscription, upstreamMessage, consumer::ack);
        }
    }

    static void process(
            Storage storage,
            MetadataSignatureVerifier metadataSignatureVerifier,
            List<Publisher> publishers,
            String topic,
            String subscription,
            PubsubMessage upstreamMessage,
            Runnable ack
    ) {
        try {
            Map<String, String> attributes = upstreamMessage.getAttributesMap();

            String payloadFormat = attributes.get("payloadFormat");
            if (!(payloadFormat.equals("JSON_API_V1") || payloadFormat.equals("DAPLA_JSON_API_V1"))) {
                throw new RuntimeException("Ignoring message. Not a valid payloadFormat");
            }

            String eventType = attributes.get("eventType");
            if (!"OBJECT_FINALIZE".equals(eventType)) {
                throw new RuntimeException("Ignoring message. eventType OBJECT_FINALIZE is the only one supported!");
            }

            JsonNode upstreamJson;
            try {
                upstreamJson = objectMapper.readTree(upstreamMessage.getData().toStringUtf8());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            String name = upstreamJson.get("name").textValue();

            Pattern pattern = Pattern.compile("(?<path>.+)/(?<version>[^/]+)/(?<filename>[^/]+)");

            Matcher m = pattern.matcher(name);
            if (!m.matches()) {
                LOG.debug("Ignored message due to name not matching pattern 'name/of/dataset/version/filename', name: {}", name);
                ack.run();
                return;
            }

            String filename = m.group("filename");
            String path = m.group("path");
            String version = m.group("version");

            if (!".dataset-meta.json.sign".equals(filename)) {
                LOG.debug("Ignored message with filename: {}", upstreamJson);
                ack.run();
                return;
            }

            String kind = upstreamJson.get("kind").textValue();

            String schemeAndAuthority;
            if ("storage#object".equals(kind)) {
                // String generation = upstreamJson.get("generation").textValue();
                // String metageneration = upstreamJson.get("metageneration").textValue();
                String bucket = upstreamJson.get("bucket").textValue();
                schemeAndAuthority = "gs://" + bucket;
            } else if ("filesystem".equals(kind)) {
                schemeAndAuthority = "file://";
            } else {
                throw new IllegalArgumentException("Illegal kind: " + kind);
            }

            LOG.debug(String.format("processing message%n  topic:        '%s'%n  subscription: '%s'%n  payload:%n%s%n", topic, subscription, upstreamJson));

            DatasetUri datasetUri = DatasetUri.of(schemeAndAuthority, path, version);

            MetadataReadAndVerifyResult metadataReadAndVerifyResult = resolveAndReadDatasetMeta(storage, metadataSignatureVerifier, datasetUri);
            if (!metadataReadAndVerifyResult.signatureValid) {
                LOG.warn("Invalid signature for metadata of dataset: {}", datasetUri.toString());
                ack.run();
                return;
            }

            final AtomicInteger succeeded = new AtomicInteger(0);

            if (publishers.isEmpty()) {
                LOG.warn("Message ignored due to no configured downstream publishers!");
                ack.run();
                return;
            }

            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataReadAndVerifyResult.datasetMetaByteString.toStringUtf8(), DatasetMeta.class);
            if (!name.endsWith(datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign")) {
                LOG.error("Path validation failed! 'name' does not end with dataset-uri matching id.path and id.version from dataset-meta.json");
                ack.run();
                return;
            }
            String parentUri = schemeAndAuthority + name.substring(0, name.lastIndexOf(datasetMeta.getId().getPath()));

            for (Publisher publisher : publishers) {
                PubsubMessage downstreamMessage = PubsubMessage.newBuilder()
                        .putAttributes("parentUri", parentUri)
                        .setData(metadataReadAndVerifyResult.datasetMetaByteString)
                        .build();
                ApiFuture<String> publishResponseFuture = publisher.publish(downstreamMessage);
                ApiFutures.addCallback(publishResponseFuture, new ApiFutureCallback<>() {
                            @Override
                            public void onSuccess(String result) {
                                if (succeeded.incrementAndGet() == publishers.size()) {
                                    ack.run();
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
        } catch (RuntimeException | Error | IOException e) {
            LOG.error("Error while processing message, upstream subscription must wait for deadline before message is eligible for re-delivery", e);
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
                    if (!publisher.awaitTermination(3, TimeUnit.SECONDS)) {
                        while (!publisher.awaitTermination(3, TimeUnit.SECONDS)) {
                        }
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
