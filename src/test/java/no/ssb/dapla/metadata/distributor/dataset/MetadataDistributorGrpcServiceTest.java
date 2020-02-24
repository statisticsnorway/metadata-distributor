package no.ssb.dapla.metadata.distributor.dataset;

import io.grpc.Channel;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class MetadataDistributorGrpcServiceTest {

    @Inject
    Channel channel;

    @Test
    void thatThisWorks() throws InterruptedException, IOException {
        MetadataDistributorServiceBlockingStub distributor = MetadataDistributorServiceGrpc.newBlockingStub(channel);

        String dataFolder = System.getProperty("user.dir") + "/target/data";

        for (int i = 0; i < 2; i++) {
            DatasetMeta datasetMeta = createDatasetMeta(dataFolder, i);

            writeDatasetMetaFile(datasetMeta);

            DataChangedRequest request = DataChangedRequest.newBuilder()
                    .setProjectId("dapla")
                    .setTopicName("file-events-1")
                    .setParentUri(datasetMeta.getParentUri())
                    .setPath(datasetMeta.getId().getPath())
                    .setVersion(datasetMeta.getId().getVersion())
                    .setFilename("dataset-meta.json")
                    .build();

            DataChangedResponse response = distributor.dataChanged(request);

            assertThat(response.getTxId()).isNotNull();
        }

        Thread.sleep(2000);
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
        Path datasetMetaJsonPath = Path.of(dataFolder + datasetMeta.getId().getPath() + "/" + datasetMeta.getId().getVersion() + "/dataset-meta.json");
        Files.createDirectories(datasetMetaJsonPath.getParent());
        String datasetMetaJson = ProtobufJsonUtils.toString(datasetMeta);
        Files.writeString(datasetMetaJsonPath, datasetMetaJson, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}