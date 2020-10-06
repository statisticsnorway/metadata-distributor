package no.ssb.dapla.metadata.distributor;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.helidon.config.Config;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.opentracing.Tracer;
import no.ssb.dapla.metadata.distributor.dataset.DefaultMetadataSignatureVerifier;
import no.ssb.dapla.metadata.distributor.dataset.MetadataDistributorService;
import no.ssb.dapla.metadata.distributor.dataset.MetadataRouter;
import no.ssb.dapla.metadata.distributor.dataset.MetadataRouterTopicAndSubscriptionInitialization;
import no.ssb.dapla.metadata.distributor.dataset.MetadataSignatureVerifier;
import no.ssb.dapla.metadata.distributor.health.Health;
import no.ssb.dapla.metadata.distributor.health.ReadinessSample;
import no.ssb.pubsub.EmulatorPubSub;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.RealPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;

import static java.util.Optional.ofNullable;

public class MetadataDistributorApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty("logback.configurationFile", logbackConfigurationFile);
        }

        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(MetadataDistributorApplication.class);
    }

    public static void initLogging() {
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new MetadataDistributorApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) this.instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) this.instanceByType.get(clazz);
    }

    final List<MetadataRouter> metadataRouters = new CopyOnWriteArrayList<>();

    MetadataDistributorApplication(Config config, Tracer tracer) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        Health health = new Health(config, lastReadySample, () -> get(WebServer.class));

        PubSub pubSub = createPubSub(config.get("pubsub"));
        put(PubSub.class, pubSub);

        MetadataSignatureVerifier metadataSignatureVerifier;
        Config signerConfig = config.get("metadatads");
        if (signerConfig.get("bypass-validation").asBoolean().orElse(false)) {
            metadataSignatureVerifier = (data, receivedSign) -> true; // always accept signature
        } else {
            String keystoreFormat = signerConfig.get("format").asString().get();
            String keystore = signerConfig.get("keystore").asString().get();
            String keyAlias = signerConfig.get("keyAlias").asString().get();
            char[] password = signerConfig.get("password-file").asString()
                    .filter(s -> !s.isBlank())
                    .map(passwordFile -> Path.of(passwordFile))
                    .filter(Files::exists)
                    .map(MetadataDistributorApplication::readPasswordFromFile)
                    .orElseGet(() -> signerConfig.get("password").asString().get().toCharArray());
            String algorithm = signerConfig.get("algorithm").asString().get();
            metadataSignatureVerifier = new DefaultMetadataSignatureVerifier(keystoreFormat, keystore, keyAlias, password, algorithm);
        }
        put(MetadataSignatureVerifier.class, metadataSignatureVerifier);

        MetadataDistributorService metadataDistributorService = new MetadataDistributorService(pubSub);
        put(MetadataDistributorService.class, metadataDistributorService);

        Storage storage = createStorage(config.get("storage.cloud-storage"));
        if (storage != null) {
            put(Storage.class, storage);
        }

        if (config.get("pubsub.admin").asBoolean().orElse(false)) {
            config.get("pubsub.metadata-routing").asNodeList().get().stream().forEach(routing -> {
                MetadataRouterTopicAndSubscriptionInitialization.initializeTopicsAndSubscriptions(routing, pubSub);
            });
        }

        config.get("pubsub.metadata-routing").asNodeList().get().stream().forEach(routing -> {
            String fileSystemDataFolder = config.get("storage.file-system.data-folder").asString().orElse("");
            metadataRouters.add(new MetadataRouter(routing, pubSub, storage, metadataSignatureVerifier, fileSystemDataFolder));
        });

        Routing routing = Routing.builder()
                //.register(AccessLogSupport.create(config.get("webserver.access-log")))
                //.register(WebTracingConfig.create(config.get("tracing")))
                //.register(ProtobufJsonSupport.create())
                //.register(MetricsSupport.create())
                //.register(health)
                //.register("/rpc/MetadataDistributorService", metadataDistributorService)
                .get("/hei", (req, res) -> {
                    res.status(200).send();
                })
                .build();
        put(Routing.class, routing);

        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    private static char[] readPasswordFromFile(Path passwordPath) {
        try {
            return Files.readString(passwordPath).toCharArray();
        } catch (IOException e) {
            return null;
        }
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
                    .createScoped(List.of("https://www.googleapis.com/auth/devstorage.read_write"));
        } else if ("compute-engine".equalsIgnoreCase(configuredProviderChoice)) {
            LOG.info("Cloud Storage running with the compute-engine google credentials provider");
            credentials = ComputeEngineCredentials.create()
                    .createScoped(List.of("https://www.googleapis.com/auth/devstorage.read_write"));
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

    public CompletionStage<MetadataDistributorApplication> start() {
        return ofNullable(this.get(WebServer.class))
                .map(WebServer::start)
                .orElse(CompletableFuture.completedFuture(null))
                .thenApply(webServer -> this);
    }

    public CompletionStage<MetadataDistributorApplication> stop() {
        return ofNullable(this.get(WebServer.class))
                .map(WebServer::shutdown)
                .orElse(CompletableFuture.completedFuture(null))
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
                .thenCombine(get(MetadataDistributorService.class).shutdown(), (app, service) -> app)
                .thenCombine(CompletableFuture.runAsync(() -> ofNullable(get(PubSub.class)).ifPresent(PubSub::close)), (a, v) -> this);
    }
}
