package no.ssb.dapla.metadata.distributor;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.config.spi.ConfigSource;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public class MetadataDistributorApplicationBuilder {
    protected Config config;

    public static Config createDefaultConfig() {
        List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList();
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            configSourceSupplierList.add(ConfigSources.file(overrideFile).optional());
        }

        configSourceSupplierList.add(ConfigSources.file("conf/application.yaml").optional());
        configSourceSupplierList.add(ConfigSources.classpath("application.yaml"));
        return Config.builder().sources(configSourceSupplierList).build();
    }

    public MetadataDistributorApplication build() {
        Config config = ofNullable(this.config).orElseGet(() -> createDefaultConfig());

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(false);
        Tracer tracer = tracerBuilder.build();

        return new MetadataDistributorApplication(config, tracer);
    }

    public <T> MetadataDistributorApplicationBuilder override(Class<T> clazz, T instance) {
        if (Config.class.isAssignableFrom(clazz)) {
            config = (Config) instance;
        }
        return this;
    }
}
