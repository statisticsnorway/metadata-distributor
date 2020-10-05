package no.ssb.dapla.metadata.distributor.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.helidon.config.Config;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.parquet.GCSReadChannelBasedInputFile;
import no.ssb.dapla.metadata.distributor.parquet.ParquetTools;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class MetadataRouter {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRouter.class);

    final PubSub pubSub;
    final Storage storage;
    final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    final List<Publisher> publishers = new CopyOnWriteArrayList<>();
    final MetadataSignatureVerifier metadataSignatureVerifier;
    final String fileSystemDataFolder;

    public MetadataRouter(Config routeConfig, PubSub pubSub, Storage storage, MetadataSignatureVerifier metadataSignatureVerifier, String fileSystemDataFolder) {
        this.pubSub = pubSub;
        this.storage = storage;
        this.metadataSignatureVerifier = metadataSignatureVerifier;
        this.fileSystemDataFolder = fileSystemDataFolder;

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
            String fileSystemDataFolder,
            MetadataSignatureVerifier metadataSignatureVerifier,
            DatasetUri datasetUri
    ) throws IOException {
        byte[] datasetMetaBytes;
        byte[] datasetDocBytes = null;
        byte[] datasetLineageBytes = null;
        byte[] datasetMetaSignatureBytes;
        String datasetMetaJsonPath = datasetUri.toURI().getPath() + "/.dataset-meta.json";
        String datasetDocJsonPath = datasetUri.toURI().getPath() + "/.dataset-doc.json";
        String datasetLinageJsonPath = datasetUri.toURI().getPath() + "/.dataset-lineage.json";
        String datasetMetaJsonSignaturePath = datasetUri.toURI().getPath() + "/.dataset-meta.json.sign";
        String scheme = datasetUri.toURI().getScheme();
        Schema avroSchema;
        switch (scheme) {
            case "file":
                datasetMetaBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetMetaJsonPath));
                if (Files.isReadable(Path.of(fileSystemDataFolder, datasetDocJsonPath))) {
                    datasetDocBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetDocJsonPath));
                }
                if (Files.isReadable(Path.of(fileSystemDataFolder, datasetLinageJsonPath))) {
                    datasetLineageBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetLinageJsonPath));
                }
                datasetMetaSignatureBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetMetaJsonSignaturePath));
                avroSchema = getAvroSchemaFromLocalFileSystem(datasetUri);
                break;
            case "gs":
                String bucket = datasetUri.toURI().getHost();
                datasetMetaBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonPath)));
                BlobId datasetDocBlobId = BlobId.of(bucket, stripLeadingSlashes(datasetDocJsonPath));
                Blob datasetDocBlob = storage.get(datasetDocBlobId);
                if (datasetDocBlob != null) {
                    datasetDocBytes = storage.readAllBytes(datasetDocBlobId);
                }
                BlobId datasetLineageBlobId = BlobId.of(bucket, stripLeadingSlashes(datasetLinageJsonPath));
                Blob datasetLineageBlob = storage.get(datasetLineageBlobId);
                if (datasetLineageBlob != null) {
                    datasetLineageBytes = storage.readAllBytes(datasetLineageBlobId);
                }

                datasetMetaSignatureBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonSignaturePath)));
                avroSchema = getAvroSchemaFromGoogleCloudStorage(storage, datasetUri);
                break;
            default:
                throw new RuntimeException("Scheme not supported. scheme='" + scheme + "'");
        }
        boolean verified = metadataSignatureVerifier.verify(datasetMetaBytes, datasetMetaSignatureBytes);
        return new MetadataReadAndVerifyResult(verified,
                ByteString.copyFrom(datasetMetaBytes),
                ofNullable(datasetDocBytes).map(ByteString::copyFrom).orElse(null),
                ofNullable(datasetLineageBytes).map(ByteString::copyFrom).orElse(null),
                avroSchema
        );
    }

    private static Schema getAvroSchemaFromLocalFileSystem(DatasetUri datasetUri) {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(datasetUri.toURI().getRawPath());
        HadoopInputFile hadoopInputFile;
        try {
            hadoopInputFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Schema schema = ParquetTools.getAvroSchemaFromFile(hadoopInputFile);
        return schema;
    }

    private static Schema getAvroSchemaFromGoogleCloudStorage(Storage storage, DatasetUri datasetUri) {
        String bucket = datasetUri.toURI().getHost();
        String prefix = datasetUri.toURI().getPath();
        Page<Blob> page = storage.list(bucket, BlobListOption.prefix(prefix), BlobListOption.pageSize(10));
        Blob firstParquetBlob = paginateUntil(page, b -> b.getName().endsWith(".parquet"));
        if (firstParquetBlob == null) {
            return null;
        }
        Schema schema = ParquetTools.getAvroSchemaFromFile(new GCSReadChannelBasedInputFile(firstParquetBlob));
        return schema;
    }

    private static Blob paginateUntil(Page<Blob> firstPage, Predicate<? super Blob> predicate) {
        return StreamSupport.stream(firstPage.iterateAll().spliterator(), false)
                .filter(predicate)
                .findFirst()
                .orElseGet(() -> {
                    Page<Blob> page = firstPage;
                    while (page.hasNextPage()) {
                        page = page.getNextPage();
                        Optional<Blob> first = StreamSupport.stream(page.iterateAll().spliterator(), false)
                                .filter(predicate)
                                .findFirst();
                        if (first.isPresent()) {
                            return first.get();
                        }
                    }
                    return null;
                });
    }

    private static String stripLeadingSlashes(String input) {
        return input.startsWith("/") ? stripLeadingSlashes(input.substring(1)) : input;
    }

    static class MetadataReadAndVerifyResult {

        final boolean signatureValid;
        final ByteString datasetMetaByteString;
        final ByteString datasetDocByteString;
        final ByteString datasetLineageByteString;
        final Schema schema;

        MetadataReadAndVerifyResult(boolean signatureValid,
                                    ByteString datasetMetaByteString,
                                    ByteString datasetDocByteString,
                                    ByteString datasetLineageByteString,
                                    Schema schema) {
            this.signatureValid = signatureValid;
            this.datasetMetaByteString = datasetMetaByteString;
            this.datasetDocByteString = datasetDocByteString;
            this.datasetLineageByteString = datasetLineageByteString;
            this.schema = schema;
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
            process(storage, fileSystemDataFolder, metadataSignatureVerifier, publishers, topic, subscription, upstreamMessage, consumer::ack);
        }
    }

    static void process(
            Storage storage,
            String fileSystemDataFolder,
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

            MetadataReadAndVerifyResult metadataReadAndVerifyResult = resolveAndReadDatasetMeta(storage, fileSystemDataFolder, metadataSignatureVerifier, datasetUri);
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

            ObjectNode downstreamMessageDataNode = objectMapper.createObjectNode();
            downstreamMessageDataNode.put("parentUri", parentUri);
            try (InputStream inputStream = metadataReadAndVerifyResult.datasetMetaByteString.newInput()) {
                JsonNode datasetMetaNode = objectMapper.readTree(inputStream);
                downstreamMessageDataNode.set("dataset-meta", datasetMetaNode);
            }
            if (metadataReadAndVerifyResult.datasetDocByteString != null) {
                try (InputStream inputStream = metadataReadAndVerifyResult.datasetDocByteString.newInput()) {
                    JsonNode datasetMetaNode = objectMapper.readTree(inputStream);
                    downstreamMessageDataNode.set("dataset-doc", datasetMetaNode);
                }
            }
            if (metadataReadAndVerifyResult.datasetLineageByteString != null) {
                try (InputStream inputStream = metadataReadAndVerifyResult.datasetLineageByteString.newInput()) {
                    JsonNode datasetMetaNode = objectMapper.readTree(inputStream);
                    downstreamMessageDataNode.set("dataset-lineage", datasetMetaNode);
                }
            }
            ByteString downstreamMessageData = ByteString.copyFrom(objectMapper.writeValueAsBytes(downstreamMessageDataNode));

            for (Publisher publisher : publishers) {
                PubsubMessage downstreamMessage = PubsubMessage.newBuilder()
                        .putAttributes("parentUri", parentUri)
                        .setData(downstreamMessageData)
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
