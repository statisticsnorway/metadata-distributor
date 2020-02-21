package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.restoreTracingContext;
import static no.ssb.helidon.application.Tracing.spanFromGrpc;

public class MetadataDistributorGrpcService extends MetadataDistributorServiceGrpc.MetadataDistributorServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDistributorGrpcService.class);

    final TransportChannelProvider channelProvider;
    final CredentialsProvider credentialsProvider;

    final Map<ProjectTopicName, Publisher> publisherByProjectTopicName = new ConcurrentHashMap<>();

    public MetadataDistributorGrpcService(TransportChannelProvider channelProvider, CredentialsProvider credentialsProvider) {
        this.channelProvider = channelProvider;
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public void dataChanged(DataChangedRequest request, StreamObserver<DataChangedResponse> responseObserver) {
        TracerAndSpan tracerAndSpan = spanFromGrpc(request, "dataChanged");
        Span span = tracerAndSpan.span();
        try {
            String projectId = request.getProjectId();
            String topicName = request.getTopicName();
            Publisher publisher = publisherByProjectTopicName.computeIfAbsent(ProjectTopicName.of(projectId, topicName), ptn -> {
                try {
                    return Publisher.newBuilder(ptn)
                            .setChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            PubsubMessage message = PubsubMessage.newBuilder().setData(request.toByteString()).build();
            ApiFuture<String> publishResponseFuture = publisher.publish(message); // async
            ApiFutures.addCallback(publishResponseFuture, new ApiFutureCallback<>() {
                @Override
                public void onFailure(Throwable t) {
                    try {
                        restoreTracingContext(tracerAndSpan);
                        LOG.error("while attempting to publish message to Google PubSub topic: " + publisher.getTopicNameString(), t);
                        Tracing.logError(span, t, "while attempting to publish message to Google PubSub", "topic", publisher.getTopicNameString());
                        responseObserver.onError(t);
                    } finally {
                        span.finish();
                    }
                }

                @Override
                public void onSuccess(String messageId) {
                    try {
                        restoreTracingContext(tracerAndSpan);
                        String txId = UUID.randomUUID().toString();
                        span.log(Map.of("event", "successfully published message", "messageId", messageId, "txId", txId));
                        responseObserver.onNext(DataChangedResponse.newBuilder().setTxId(txId).build());
                        responseObserver.onCompleted();
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());
        } catch (RuntimeException | Error e) {
            try {
                Tracing.logError(span, e);
                LOG.error("unexpected error", e);
                responseObserver.onError(e);
            } finally {
                span.finish();
            }
        }
    }

    public CompletableFuture<MetadataDistributorGrpcService> shutdown() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                for (Publisher publisher : publisherByProjectTopicName.values()) {
                    publisher.shutdown();
                }
                for (Publisher publisher : publisherByProjectTopicName.values()) {
                    while (!publisher.awaitTermination(5, TimeUnit.SECONDS)) {
                    }
                }
                return this;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
