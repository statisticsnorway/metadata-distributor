package no.ssb.dapla.metadata.distributor;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import no.ssb.dapla.metadata.distributor.dataset.MetadataRouterTopicAndSubscriptionInitialization;
import no.ssb.dapla.metadata.distributor.dataset.MetadataSignatureVerifier;
import no.ssb.dapla.metadata.distributor.health.Health;
import no.ssb.dapla.metadata.distributor.health.ReadinessSample;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonGrpcWebTranscoding;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import no.ssb.pubsub.EmulatorPubSub;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.RealPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;

public class MetadataDistributorApplication extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(MetadataDistributorApplication.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new MetadataDistributorApplicationBuilder().build()
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

    MetadataDistributorApplication(Config config, Tracer tracer) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        Health health = new Health(config, lastReadySample, () -> get(WebServer.class));

        PubSub pubSub = createPubSub(config.get("pubsub"));
        put(PubSub.class, pubSub);

        Config signerConfig = config.get("metadatads");
        String keystoreFormat = signerConfig.get("format").asString().get();
        String keystore = signerConfig.get("keystore").asString().get();
        String keyAlias = signerConfig.get("keyAlias").asString().get();
        char[] password = signerConfig.get("password").asString().get().toCharArray();
        String algorithm = signerConfig.get("algorithm").asString().get();
        MetadataSignatureVerifier metadataSignatureVerifier = new MetadataSignatureVerifier(keystoreFormat, keystore, keyAlias, password, algorithm);
        put(MetadataSignatureVerifier.class, metadataSignatureVerifier);

        MetadataDistributorGrpcService distributorGrpcService = new MetadataDistributorGrpcService(pubSub);
        put(MetadataDistributorGrpcService.class, distributorGrpcService);

        Storage storage = createStorage(config.get("cloud-storage"));

        config.get("pubsub.metadata-routing").asNodeList().get().stream().forEach(routing -> {
            MetadataRouterTopicAndSubscriptionInitialization.initializeTopicsAndSubscriptions(routing, pubSub);
        });

        config.get("pubsub.metadata-routing").asNodeList().get().stream().forEach(routing -> {
            metadataRouters.add(new MetadataRouter(routing, pubSub, storage, metadataSignatureVerifier));
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

    static PubSub createPubSub(Config config) {
        boolean useEmulator = config.get("use-emulator").asBoolean().orElse(false);
        if (useEmulator) {
            Config emulatorConfig = config.get("emulator");
            String host = emulatorConfig.get("host").asString().get();
            int port = emulatorConfig.get("port").asInt().get();
            return new EmulatorPubSub(host, port);
        } else {
            String configuredProviderChoice = config.get("credential-provider").asString().orElse("default");
            if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
                LOG.info("PubSub running with the service-account google credentials provider");
                String serviceAccountKeyPath = config.get("credentials.service-account.path").asString().orElse(null);
                return RealPubSub.createWithServiceAccountKeyCredentials(serviceAccountKeyPath);
            } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
                LOG.info("PubSub Running with the compute-engine google credentials provider");
                return RealPubSub.createWithComputeEngineCredentials();
            } else { // default
                LOG.info("PubSub Running with the default google credentials provider");
                return RealPubSub.createWithDefaultCredentials();
            }
        }
    }

    static Storage createStorage(Config config) {
        if (!config.get("enabled").asBoolean().orElse(false)) {
            return null;
        }
        Credentials credentials = null;
        String configuredProviderChoice = config.get("credential-provider").asString().orElse("default");
        if ("service-account".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Cloud Storage running with the service-account google credentials provider");
            String serviceAccountKeyPath = config.get("credentials.service-account.path").asString().orElse(null);
            credentials = createWithServiceAccountKeyCredentials(serviceAccountKeyPath)
                    .createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
        } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Cloud Storage running with the compute-engine google credentials provider");
            credentials = ComputeEngineCredentials.create()
                    .createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
        }
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        return storage;
    }

    public static GoogleCredentials createWithServiceAccountKeyCredentials(String serviceAccountKeyPath) {
        try {
            Path serviceAccountKeyFilePath = Path.of(serviceAccountKeyPath);
            return ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyFilePath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletionStage<HelidonApplication> stop() {
        return super.stop()
                .thenCombine(CompletableFuture.runAsync(() -> {
                    List<CompletableFuture<MetadataRouter>> metadataRouterFutures = new ArrayList<>();
                    for (MetadataRouter metadataRouter : metadataRouters) {
                        metadataRouterFutures.add(metadataRouter.shutdown());
                    }
                    CompletableFuture<Void> allMetadataRouterFutures = CompletableFuture.allOf(metadataRouterFutures.toArray(new CompletableFuture[0]));
                    allMetadataRouterFutures
                            .orTimeout(10, TimeUnit.SECONDS)
                            .join();
                }), (app, v) -> this)
                .thenCombine(get(MetadataDistributorGrpcService.class).shutdown(), (app, service) -> app)
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(PubSub.class)).ifPresent(PubSub::close)), (a, v) -> this);
    }
}
