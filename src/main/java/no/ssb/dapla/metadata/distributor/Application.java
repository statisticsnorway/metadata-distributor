package no.ssb.dapla.metadata.distributor;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.grpc.server.GrpcTracingConfig;
import io.helidon.grpc.server.ServerRequestAttribute;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import no.ssb.dapla.metadata.distributor.dataset.MetadataDistributorGrpcService;
import no.ssb.dapla.metadata.distributor.dataset.MetadataRouter;
import no.ssb.dapla.metadata.distributor.health.Health;
import no.ssb.dapla.metadata.distributor.health.ReadinessSample;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonGrpcWebTranscoding;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Application extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new ApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    final List<MetadataRouter> metadataRouters = new CopyOnWriteArrayList<>();

    Application(Config config, Tracer tracer) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        Health health = new Health(config, lastReadySample, () -> get(WebServer.class));

        String hostport = System.getenv("PUBSUB_EMULATOR_HOST");
        if (hostport == null) {
            hostport = "localhost:8538";
        }

        ManagedChannel pubSubChannel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        put(ManagedChannel.class, pubSubChannel);

        FixedTransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(pubSubChannel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        MetadataDistributorGrpcService distributorGrpcService = new MetadataDistributorGrpcService(channelProvider, credentialsProvider);
        put(MetadataDistributorGrpcService.class, distributorGrpcService);

        config.get("metadata-routing").asNodeList().get().stream().forEach(routing -> {
            metadataRouters.add(new MetadataRouter(routing, channelProvider, credentialsProvider));
        });

        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.builder(config.get("grpcserver"))
                        .tracer(tracer)
                        .tracingConfig(GrpcTracingConfig.builder()
                                .withStreaming()
                                .withVerbosity()
                                .withOperationName(new OperationNameConstructor() {
                                    @Override
                                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                                        return "Grpc server received " + method.getFullMethodName();
                                    }
                                })
                                .withTracedAttributes(ServerRequestAttribute.CALL_ATTRIBUTES,
                                        ServerRequestAttribute.HEADERS,
                                        ServerRequestAttribute.METHOD_NAME)
                                .build()
                        ),
                GrpcRouting.builder()
                        .intercept(new AuthorizationInterceptor())
                        .register(distributorGrpcService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(health)
                .register("/rpc", new HelidonGrpcWebTranscoding(
                        () -> ManagedChannelBuilder
                                .forAddress("localhost", Optional.of(grpcServer)
                                        .filter(GrpcServer::isRunning)
                                        .map(GrpcServer::port)
                                        .orElseThrow())
                                .usePlaintext()
                                .build(),
                        distributorGrpcService
                ))
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    @Override
    public CompletionStage<HelidonApplication> stop() {
        return super.stop()
                .thenCombine(CompletableFuture.supplyAsync(() -> {

                    List<CompletableFuture<MetadataRouter>> metadataRouterFutures = new ArrayList<>();
                    for (MetadataRouter metadataRouter : metadataRouters) {
                        metadataRouterFutures.add(metadataRouter.shutdown());
                    }
                    CompletableFuture<Void> allMetadataRouterFutures = CompletableFuture.allOf(metadataRouterFutures.toArray(new CompletableFuture[0]));
                    allMetadataRouterFutures
                            .orTimeout(10, TimeUnit.SECONDS)
                            .join();

                    ManagedChannel channel = get(ManagedChannel.class);
                    channel.shutdown();
                    try {
                        if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                            channel.shutdownNow();
                            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                                throw new RuntimeException("Unable to close channel");
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return this;
                }), (app1, app2) -> app1)
                .thenCombine(get(MetadataDistributorGrpcService.class).shutdown(), (app, service) -> app);
    }
}
