package no.ssb.dapla.metadata.distributor.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplication;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.PubSubAdmin;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class FilesystemMetadataPropagationTest {

    @Inject
    MetadataDistributorApplication application;

    @Inject
    TestClient client;

    static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void thatThisWorks() throws IOException, InterruptedException {
        initTopicAndSubscription("dapla", "catalog-1", "catalog-1-junit");

        String dataFolder = System.getProperty("user.dir") + "/target/data";
        DatasetMeta datasetMeta = createDatasetMeta(0);

        // copy parquet file to folder
        Files.createDirectories(Path.of(dataFolder, datasetMeta.getId().getPath(), datasetMeta.getId().getVersion()));
        Files.copy(
                Path.of("src/test/resources/no/ssb/dapla/metadata/distributor/parquet/dataset.parquet"),
                Path.of(dataFolder, datasetMeta.getId().getPath(), datasetMeta.getId().getVersion(), "dataset.parquet")
        );

        writeContentAsUtf8ToFile(dataFolder, datasetMeta, ".dataset-meta.json", ProtobufJsonUtils.toString(datasetMeta));
        String metadataPath = datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json";
        DataChangedRequest request = DataChangedRequest.newBuilder()
                .setProjectId("dapla")
                .setTopicName("file-events-1")
                .setUri("file:" + dataFolder + metadataPath)
                .build();

        DataChangedResponse response = client.postJson("/rpc/MetadataDistributorService/dataChanged", request, DataChangedResponse.class).expect200Ok().body();
        assertThat(response.getMessageId()).isNotNull();

        writeContentAsUtf8ToFile(dataFolder, datasetMeta, ".dataset-doc.json", "{}");

        ByteString validMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(datasetMeta));
        MetadataSigner metadataSigner = new MetadataSigner("PKCS12", "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair", "changeit".toCharArray(), "SHA256withRSA");
        byte[] signature = metadataSigner.sign(validMetadataJson.toByteArray());
        writeContentToFile(dataFolder, datasetMeta, ".dataset-meta.json.sign", signature);
        String metadataSignaturePath = datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign";
        DataChangedRequest signatureRequest = DataChangedRequest.newBuilder()
                .setProjectId("dapla")
                .setTopicName("file-events-1")
                .setUri("file:" + dataFolder + metadataSignaturePath)
                .build();
        DataChangedResponse signatureResponse = client.postJson("/rpc/MetadataDistributorService/dataChanged", signatureRequest, DataChangedResponse.class).expect200Ok().body();
        assertThat(signatureResponse.getMessageId()).isNotNull();

        final CountDownLatch latch = new CountDownLatch(1);

        MessageReceiver messageReceiver = (message, consumer) -> {
            try {
                JsonNode messageDataNode;
                try (InputStream inputStream = message.getData().newInput()) {
                    messageDataNode = objectMapper.readTree(inputStream);
                }
                String datasetMetaJson = objectMapper.writeValueAsString(messageDataNode.get("dataset-meta"));
                DatasetMeta propagatedMetadata = ProtobufJsonUtils.toPojo(datasetMetaJson, DatasetMeta.class);
                System.out.format("Received message in junit test: %s%n", messageDataNode.toPrettyString());
                if (propagatedMetadata.equals(datasetMeta)) {
                    latch.countDown();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                consumer.ack();
            }
        };

        PubSub pubSub = application.get(PubSub.class);
        Subscriber subscriber = pubSub.getSubscriber("dapla", "catalog-1-junit", messageReceiver);
        subscriber.addListener(
                new Subscriber.Listener() {
                    public void failed(Subscriber.State from, Throwable failure) {
                        System.out.println("Error with subscriber");
                    }
                },
                MoreExecutors.directExecutor());
        ApiService subscriberStartAsyncApiService = subscriber.startAsync();

        subscriberStartAsyncApiService.awaitRunning();

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    private void initTopicAndSubscription(String projectId, String topic, String subscription) {
        PubSub pubSub = application.get(PubSub.class);
        try (TopicAdminClient topicAdminClient = pubSub.getTopicAdminClient()) {
            PubSubAdmin.createTopicIfNotExists(topicAdminClient, projectId, topic);
            try (SubscriptionAdminClient subscriptionAdminClient = pubSub.getSubscriptionAdminClient()) {
                PubSubAdmin.createSubscriptionIfNotExists(subscriptionAdminClient, projectId, topic, subscription, 60);
            }
        }
    }

    private DatasetMeta createDatasetMeta(int i) {
        String path = "/path/to/dataset-" + i;
        String version = String.valueOf(System.currentTimeMillis());
        return DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath(path)
                        .setVersion(version)
                        .build())
                .setType(Type.BOUNDED)
                .setValuation(Valuation.OPEN)
                .setState(DatasetState.INPUT)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .setCreatedBy("junit")
                .build();
    }

    private void writeContentAsUtf8ToFile(String dataFolder, DatasetMeta datasetMeta, String filename, String content) throws IOException {
        Path path = Path.of(dataFolder + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/" + filename);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void writeContentToFile(String dataFolder, DatasetMeta datasetMeta, String filename, byte[] content) throws IOException {
        Path datasetMetaJsonPath = Path.of(dataFolder + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/" + filename);
        Files.createDirectories(datasetMetaJsonPath.getParent());
        Files.write(datasetMetaJsonPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}