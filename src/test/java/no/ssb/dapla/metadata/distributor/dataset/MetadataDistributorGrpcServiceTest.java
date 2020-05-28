package no.ssb.dapla.metadata.distributor.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.Channel;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplication;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.EmulatorPubSub;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.PubSubAdmin;
import no.ssb.testing.helidon.ConfigOverride;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ConfigOverride({
        "pubsub.metadata-routing.0.upstream.0.subscribe", "false",
})
@ExtendWith(IntegrationTestExtension.class)
class MetadataDistributorGrpcServiceTest {

    @Inject
    MetadataDistributorApplication application;

    @Inject
    Channel channel;

    @Test
    void thatThisWorks() throws IOException {
        MetadataSigner metadataSigner = new MetadataSigner("PKCS12", "src/test/resources/metadata-signer_keystore.p12",
                "dataAccessKeyPair", "changeit".toCharArray(), "SHA256withRSA");

        initTopicAndSubscription("dapla", "file-events-1", "junit");

        MetadataDistributorServiceBlockingStub distributor = MetadataDistributorServiceGrpc.newBlockingStub(channel);

        String dataFolder = System.getProperty("user.dir") + "/target/data";

        Set<String> messageIds = new LinkedHashSet<>();

        for (int i = 0; i < 2; i++) {
            DatasetMeta datasetMeta = createDatasetMeta(i);

            writeContentAsUtf8ToFile(dataFolder, datasetMeta, ".dataset-meta.json", ProtobufJsonUtils.toString(datasetMeta));
            writeContentAsUtf8ToFile(dataFolder, datasetMeta, ".dataset-doc.json", "{}");

            String parentUri = "file:" + dataFolder;

            {
                DataChangedRequest request = DataChangedRequest.newBuilder()
                        .setProjectId("dapla")
                        .setTopicName("file-events-1")
                        .setUri(parentUri + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json")
                        .build();

                DataChangedResponse response = distributor.dataChanged(request);

                assertThat(response.getMessageId()).isNotNull();

                messageIds.add(response.getMessageId());
            }
            {
                DataChangedRequest request = DataChangedRequest.newBuilder()
                        .setProjectId("dapla")
                        .setTopicName("file-events-1")
                        .setUri(parentUri + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-doc.json")
                        .build();

                DataChangedResponse response = distributor.dataChanged(request);

                assertThat(response.getMessageId()).isNotNull();

                messageIds.add(response.getMessageId());
            }

            ByteString validMetadataJson = ByteString.copyFromUtf8(ProtobufJsonUtils.toString(datasetMeta));
            byte[] signature = metadataSigner.sign(validMetadataJson.toByteArray());

            writeContentToFile(dataFolder, datasetMeta, ".dataset-meta.json.sign", signature);

            DataChangedRequest signatureRequest = DataChangedRequest.newBuilder()
                    .setProjectId("dapla")
                    .setTopicName("file-events-1")
                    .setUri(parentUri + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/.dataset-meta.json.sign")
                    .build();

            DataChangedResponse signatureResponse = distributor.dataChanged(signatureRequest);

            assertThat(signatureResponse.getMessageId()).isNotNull();

            messageIds.add(signatureResponse.getMessageId());
        }

        ObjectMapper mapper = new ObjectMapper();
        try (SubscriberStub subscriber = getSubscriberStub()) {
            String subscriptionName = ProjectSubscriptionName.format("dapla", "junit");
            PullRequest pullRequest = PullRequest.newBuilder()
                    .setMaxMessages(1)
                    .setReturnImmediately(false)
                    .setSubscription(subscriptionName)
                    .build();

            while (!messageIds.isEmpty()) {
                PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
                List<String> ackIds = new ArrayList<>();

                for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                    PubsubMessage pubsubMessage = message.getMessage();
                    JsonNode node = mapper.readTree(pubsubMessage.getData().toByteArray());
                    System.out.format("Received message in junit test: %s%n", node.toPrettyString());
                    messageIds.remove(pubsubMessage.getMessageId());
                    ackIds.add(message.getAckId());
                }

                AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionName)
                        .addAllAckIds(ackIds)
                        .build();

                subscriber.acknowledgeCallable().call(acknowledgeRequest);
            }
        }
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

    private SubscriberStub getSubscriberStub() throws IOException {
        PubSub pubSub = application.get(PubSub.class);
        SubscriberStubSettings subscriberStubSettings;
        if (pubSub instanceof EmulatorPubSub) {
            subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(((EmulatorPubSub) pubSub).getChannelProvider())
                    .setCredentialsProvider(pubSub.getCredentialsProvider())
                    .build();
        } else {
            subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                            .setMaxInboundMessageSize(20 << 20) // 20MB
                            .build())
                    .setCredentialsProvider(pubSub.getCredentialsProvider())
                    .build();
        }

        return GrpcSubscriberStub.create(subscriberStubSettings);
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