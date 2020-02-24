module no.ssb.dapla.metadata.distributor {
    requires org.slf4j;
    requires jul.to.slf4j;
    requires org.reactivestreams;
    requires io.helidon.webserver;
    requires io.helidon.webserver.accesslog;
    requires io.helidon.config;
    requires java.net.http;
    requires io.helidon.common.reactive;
    requires logback.classic;
    requires io.helidon.metrics;
    requires io.helidon.health;
    requires io.helidon.health.checks;
    requires io.helidon.tracing;
    requires io.grpc;

    requires com.google.common;

    requires gax.grpc.and.proto.google.common.protos;

    requires grpc.protobuf;
    requires io.helidon.grpc.server;
    requires java.logging;

    requires no.ssb.helidon.application;
    requires com.google.gson; // required by JsonFormat in protobuf-java-util for serialization and deserialization

    /*
     * Not so well documented requirements are declared here to force fail-fast with proper error message if
     * missing from jvm.
     */

    requires jdk.unsupported; // required by netty to allow reliable low-level API access to direct-buffers
    requires jdk.naming.dns; // required by netty dns libraries used by reactive postgres
    requires java.sql; // required by flyway
    requires io.helidon.microprofile.config; // metrics uses provider org.eclipse.microprofile.config.spi.ConfigProviderResolver
    requires perfmark.api; // needed by grpc-client
    requires javax.inject; // required by io.helidon.grpc.server
    requires java.annotation;
    requires com.google.protobuf.util;
    requires no.ssb.helidon.media.protobuf.json.server;
    requires com.fasterxml.jackson.databind;  // required by logstash-encoder

    requires org.checkerframework.checker.qual;
    requires jaeger.core;

    requires no.ssb.dapla.metadata.distributor.protobuf;

    requires gax;
    requires com.google.api.apicommon;
    requires google.cloud.pubsub;
    requires proto.google.cloud.pubsub.v1;
    requires no.ssb.dapla.dataset.api;
    requires com.google.auth.oauth2;
    requires com.google.auth;

    provides no.ssb.helidon.application.HelidonApplicationBuilder with no.ssb.dapla.metadata.distributor.ApplicationBuilder;
}