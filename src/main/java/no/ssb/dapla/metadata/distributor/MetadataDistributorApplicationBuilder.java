package no.ssb.dapla.metadata.distributor;

import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolverRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import io.helidon.config.Config;
import io.helidon.config.spi.ConfigSource;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;
import static java.util.Optional.ofNullable;

public class MetadataDistributorApplicationBuilder {
    protected Config config;

    public static void applyGrpcProvidersWorkaround() {
        // The shaded version of grpc from helidon does not include the service definition for
        // PickFirstLoadBalancerProvider. This result in LoadBalancerRegistry not being able to
        // find it. We register them manually here.
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        // LoadBalancerRegistry.getDefaultRegistry().register(new HealthCheckingRoundRobinLoadBalancerProvider());

        // The same thing happens with the name resolvers.
        NameResolverRegistry.getDefaultRegistry().register(new DnsNameResolverProvider());
    }

    public static Config createDefaultConfig() {
        Config.Builder builder = Config.builder();
        List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList();
        String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
        if (overrideFile != null) {
            builder.addSource(file(overrideFile).optional());
        }
        return builder
                .addSource(file("conf/application.yaml").optional())
                .addSource(classpath("application.yaml"))
                .build();
    }

    public MetadataDistributorApplication build() {
        applyGrpcProvidersWorkaround();

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
