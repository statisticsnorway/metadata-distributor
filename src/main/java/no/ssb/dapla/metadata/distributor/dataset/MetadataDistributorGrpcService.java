package no.ssb.dapla.metadata.distributor.dataset;

import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.helidon.application.TracerAndSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static no.ssb.helidon.application.Tracing.spanFromGrpc;

public class MetadataDistributorGrpcService extends MetadataDistributorServiceGrpc.MetadataDistributorServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDistributorGrpcService.class);

    @Override
    public void dataChanged(DataChangedRequest request, StreamObserver<DataChangedResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "dataChanged");
        Span span = tracerAndSpan.span();
        try {
            responseObserver.onNext(DataChangedResponse.newBuilder().setTxId(UUID.randomUUID().toString()).build());
            responseObserver.onCompleted();
        } catch (RuntimeException | Error e) {
            responseObserver.onError(e);
        } finally {
            span.finish();
        }
    }
}
