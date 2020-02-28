package no.ssb.dapla.metadata.distributor.dataset;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.Channel;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.metadata.distributor.Application;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.EmulatorPubSub;
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class MetadataDistributorGrpcServiceTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @Test
    void thatThisWorks() throws IOException {
        initTopicAndSubscription("dapla", "file-events-1", "junit");

        MetadataDistributorServiceBlockingStub distributor = MetadataDistributorServiceGrpc.newBlockingStub(channel);

        String dataFolder = System.getProperty("user.dir") + "/target/data";

        Set<String> messageIds = new LinkedHashSet<>();

        for (int i = 0; i < 2; i++) {
            DatasetMeta datasetMeta = createDatasetMeta(dataFolder, i);

            writeDatasetMetaFile(datasetMeta);

            DataChangedRequest request = DataChangedRequest.newBuilder()
                    .setProjectId("dapla")
                    .setTopicName("file-events-1")
                    .setParentUri(datasetMeta.getParentUri())
                    .setPath(datasetMeta.getId().getPath())
                    .setVersion(datasetMeta.getId().getVersion())
                    .setFilename(".dataset-meta.json")
                    .build();

            DataChangedResponse response = distributor.dataChanged(request);

            assertThat(response.getTxId()).isNotNull();

            messageIds.add(response.getTxId());
        }

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
                    DataChangedRequest dataChangedRequest = DataChangedRequest.parseFrom(pubsubMessage.getData());
                    System.out.format("Received message in junit test: %s%n", ProtobufJsonUtils.toString(dataChangedRequest));
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
                    .setCredentialsProvider(((EmulatorPubSub) pubSub).getCredentialsProvider())
                    .build();
        } else {
            subscriberStubSettings = SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                            .setMaxInboundMessageSize(20 << 20) // 20MB
                            .build())
                    .build();
        }

        return GrpcSubscriberStub.create(subscriberStubSettings);
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
}