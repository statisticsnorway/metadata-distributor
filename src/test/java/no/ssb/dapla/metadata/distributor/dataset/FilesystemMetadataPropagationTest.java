package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplication;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.PubSubAdmin;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.io.IOException;
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
    Channel channel;

    @Test
    void thatThisWorks() throws IOException, InterruptedException {
        initTopicAndSubscription("dapla", "catalog-1", "catalog-1-junit");

        MetadataDistributorServiceBlockingStub distributor = MetadataDistributorServiceGrpc.newBlockingStub(channel);

        String dataFolder = System.getProperty("user.dir") + "/target/data";
        DatasetMeta datasetMeta = createDatasetMeta(dataFolder, 0);
        writeDatasetMetaFile(datasetMeta);
        String metadataPath = datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json";
        DataChangedRequest request = DataChangedRequest.newBuilder()
                .setProjectId("dapla")
                .setTopicName("file-events-1")
                .setUri(datasetMeta.getParentUri() + metadataPath)
                .build();
        DataChangedResponse response = distributor.dataChanged(request);
        assertThat(response.getMessageId()).isNotNull();

        ByteString validMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(datasetMeta));
        MetadataSigner metadataSigner = new MetadataSigner("PKCS12", "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair", "changeit".toCharArray(), "SHA256withRSA");
        byte[] signature = metadataSigner.sign(validMetadataJson.toByteArray());
        writeSignatureFile(datasetMeta, signature);
        String metadataSignaturePath = datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign";
        DataChangedRequest signatureRequest = DataChangedRequest.newBuilder()
                .setProjectId("dapla")
                .setTopicName("file-events-1")
                .setUri(datasetMeta.getParentUri() + metadataSignaturePath)
                .build();
        DataChangedResponse signatureResponse = distributor.dataChanged(signatureRequest);
        assertThat(signatureResponse.getMessageId()).isNotNull();

        final CountDownLatch latch = new CountDownLatch(1);

        MessageReceiver messageReceiver = (message, consumer) -> {
            try {
                DatasetMeta propagatedMetadata = ProtobufJsonUtils.toPojo(message.getData().toStringUtf8(), DatasetMeta.class);
                System.out.format("Received message in junit test: %s%n", propagatedMetadata);
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

    private DatasetMeta createDatasetMeta(String dataFolder, int i) {
        String parentUri = "file:" + dataFolder;
        String path = "/path/to/dataset-" + i;
        long version = System.currentTimeMillis();
        return DatasetMeta.newBuilder()
                .setId(DatasetId.newBuilder()
                        .setPath(path)
                        .setVersion(version)
                        .build())
                .setType(DatasetMeta.Type.BOUNDED)
                .setValuation(DatasetMeta.Valuation.OPEN)
                .setState(DatasetMeta.DatasetState.INPUT)
                .setParentUri(parentUri)
                .setPseudoConfig(PseudoConfig.newBuilder().build())
                .setCreatedBy("junit")
                .build();
    }

    private void writeDatasetMetaFile(DatasetMeta datasetMeta) throws IOException {
        String dataFolder = datasetMeta.getParentUri().substring("file:".length());
        Path datasetMetaJsonPath = Path.of(dataFolder + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json");
        Files.createDirectories(datasetMetaJsonPath.getParent());
        String datasetMetaJson = ProtobufJsonUtils.toString(datasetMeta);
        Files.writeString(datasetMetaJsonPath, datasetMetaJson, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void writeSignatureFile(DatasetMeta datasetMeta, byte[] signature) throws IOException {
        String dataFolder = datasetMeta.getParentUri().substring("file:".length());
        Path datasetMetaJsonPath = Path.of(dataFolder + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign");
        Files.createDirectories(datasetMetaJsonPath.getParent());
        Files.write(datasetMetaJsonPath, signature, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}