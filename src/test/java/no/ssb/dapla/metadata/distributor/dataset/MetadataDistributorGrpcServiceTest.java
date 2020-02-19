package no.ssb.dapla.metadata.distributor.dataset;

import io.grpc.Channel;
import no.ssb.dapla.metadata.distributor.Application;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class MetadataDistributorGrpcServiceTest {

    @Inject
    Application application;

    @Inject
    Channel channel;

    @Test
    void thatThisWorks() {
        MetadataDistributorServiceBlockingStub distributor = MetadataDistributorServiceGrpc.newBlockingStub(channel);
        DataChangedResponse response = distributor.dataChanged(DataChangedRequest.newBuilder()
                .setParentUri("gs://my-bucket")
                .setPath("/path/to/dataset")
                .setVersion(1582106920814L)
                .setFilename("metadata.json")
                .build());
        assertThat(response.getTxId()).isNotNull();
    }
}