package no.ssb.testing.helidon;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.config.spi.ConfigSource;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplication;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplicationBuilder;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class IntegrationTestExtension implements BeforeEachCallback, BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG;

    static {
        MetadataDistributorApplication.initLogging();
        LOG = LoggerFactory.getLogger(IntegrationTestExtension.class);
    }

    TestClient client;
    MetadataDistributorApplication application;

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        Class<?> testClass = extensionContext.getRequiredTestClass();

        List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList<>();
        ConfigOverride configOverride = testClass.getDeclaredAnnotation(ConfigOverride.class);
        if (configOverride != null) {
            String[] overrideArray = configOverride.value();
            Map<String, String> configOverrideMap = new LinkedHashMap<>();
            for (int i = 0; i < overrideArray.length; i += 2) {
                configOverrideMap.put(overrideArray[i], overrideArray[i + 1]);
            }
            configSourceSupplierList.add(ConfigSources.create(configOverrideMap));
        }
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            configSourceSupplierList.add(file(overrideFile).optional());
        }
        String profile = System.getenv("HELIDON_CONFIG_PROFILE");
        if (profile == null) {
            profile = "dev";
        }
        if (profile.equalsIgnoreCase("dev")) {
            configSourceSupplierList.add(classpath("application-dev.yaml"));
        } else if (profile.equalsIgnoreCase("azure")) {
            configSourceSupplierList.add(classpath("application-azure.yaml"));
        } else {
            // default to dev
            configSourceSupplierList.add(classpath("application-dev.yaml"));
        }
        configSourceSupplierList.add(classpath("application.yaml"));
        Config config = Config.builder().sources(configSourceSupplierList).build();

        MetadataDistributorApplicationBuilder applicationBuilder = new MetadataDistributorApplicationBuilder();
        applicationBuilder.override(Config.class, config);

        long startTime = System.currentTimeMillis();
        application = applicationBuilder.build();
        application.start().toCompletableFuture().get(5, TimeUnit.SECONDS);

        WebServer webServer = application.get(WebServer.class);
        if (webServer != null) {
            int port = webServer.port();
            LOG.info("Webserver running at port: {}, started in {} ms", port, System.currentTimeMillis() - startTime);
            client = TestClient.newClient("localhost", port);
        }
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        Object test = extensionContext.getRequiredTestInstance();
        Field[] fields = test.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!field.isAnnotationPresent(Inject.class)) {
                continue;
            }
            // application
            if (MetadataDistributorApplication.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    if (field.get(test) == null) {
                        field.set(test, application);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            if (TestClient.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    if (field.get(test) == null) {
                        field.set(test, client);
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (application != null) {
            application.stop();
        }
    }
}
