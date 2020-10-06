package no.ssb.dapla.metadata.distributor.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.stub.StreamObserver;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedResponse;
import no.ssb.helidon.application.TracerAndSpan;
import no.ssb.helidon.application.Tracing;
import no.ssb.pubsub.PubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static no.ssb.helidon.application.Tracing.restoreTracingContext;
import static no.ssb.helidon.application.Tracing.spanFromHttp;

public class MetadataDistributorService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataDistributorService.class);

    final PubSub pubSub;
    final Map<ProjectTopicName, Publisher> publisherByProjectTopicName = new ConcurrentHashMap<>();
    final ObjectMapper mapper = new ObjectMapper();

    public MetadataDistributorService(PubSub pubSub) {
        this.pubSub = pubSub;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.post("/dataChanged", Handler.create(DataChangedRequest.class, this::dataChanged));
    }

    private void dataChanged(ServerRequest req, ServerResponse res, DataChangedRequest request) {
        StreamObserver<DataChangedResponse> responseObserver;
        TracerAndSpan tracerAndSpan = spanFromHttp(req, "dataChanged");
        Span span = tracerAndSpan.span();
        try {
            String projectId = request.getProjectId();
            String topicName = request.getTopicName();
            Publisher publisher = publisherByProjectTopicName.computeIfAbsent(ProjectTopicName.of(projectId, topicName), ptn -> {
                LOG.info("Creating publisher on topic: {}", ptn.toString());
                return pubSub.getPublisher(projectId, topicName);
            });

            URI uri = new URI(request.getUri());

            Map<String, String> attributes;

            ObjectNode dataNode = mapper.createObjectNode();
            if ("gs".equals(uri.getScheme())) {
                attributes = Map.of(
                        "eventType", "OBJECT_FINALIZE",
                        "payloadFormat", "DAPLA_JSON_API_V1",
                        "bucketId", uri.getHost(),
                        "objectId", uri.getPath()
                );
                dataNode.put("kind", "storage#object");
                dataNode.put("bucket", uri.getHost());
                dataNode.put("name", uri.getPath());
            } else if ("file".equals(uri.getScheme())) {
                attributes = Map.of(
                        "eventType", "OBJECT_FINALIZE",
                        "payloadFormat", "DAPLA_JSON_API_V1",
                        "objectId", uri.getPath()
                );
                dataNode.put("kind", "filesystem");
                dataNode.put("name", uri.getPath());
            } else {
                throw new IllegalArgumentException("Invalid uri scheme: " + uri.getScheme());
            }

            PubsubMessage message = PubsubMessage.newBuilder()
                    .putAllAttributes(attributes)
                    .setData(ByteString.copyFrom(mapper.writeValueAsBytes(dataNode)))
                    .build();

            ApiFuture<String> publishResponseFuture = publisher.publish(message); // async

            ApiFutures.addCallback(publishResponseFuture, new ApiFutureCallback<>() {
                @Override
                public void onSuccess(String messageId) {
                    try {
                        restoreTracingContext(tracerAndSpan);
                        span.log(Map.of("event", "successfully published message", "messageId", messageId));
                        res.send(DataChangedResponse.newBuilder().setMessageId(messageId).build());
                    } finally {
                        span.finish();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        restoreTracingContext(tracerAndSpan);
                        String errorMsg = "while attempting to publish message to Google PubSub topic: " + publisher.getTopicNameString();
                        LOG.error(errorMsg, t);
                        Tracing.logError(span, t, "while attempting to publish message to Google PubSub", "topic", publisher.getTopicNameString());
                        res.status(400).send(errorMsg);
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());
        } catch (RuntimeException | Error | URISyntaxException | JsonProcessingException e) {
            try {
                Tracing.logError(span, e);
                LOG.error("unexpected error", e);
                res.status(500).send("unexpected error");
            } finally {
                span.finish();
            }
        }
    }

    public CompletableFuture<MetadataDistributorService> shutdown() {
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
