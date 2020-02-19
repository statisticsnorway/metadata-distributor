FROM statisticsnorway/alpine-jdk13-buildtools:latest as build

#
# Build stripped JVM
#
RUN ["jlink", "--strip-java-debug-attributes", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
 "--add-modules", "java.base,java.management,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki"]

#
# Build Application image
#
FROM alpine:latest

#
# Resources from build image
#
COPY --from=build /linked /jdk/
COPY target/libs /app/lib/
COPY target/metadata-distributor-*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10160
EXPOSE 10168

CMD ["java", "--add-exports=io.grpc/io.opencensus.common=gax", "--add-exports=io.grpc/io.opencensus.trace=gax", "--add-exports=io.grpc/io.opencensus.trace=com.google.api.client", "--add-exports=io.grpc/io.opencensus.trace.propagation=com.google.api.client", "--add-exports=io.grpc/io.opencensus.trace.export=com.google.api.client", "--add-exports=io.grpc/io.opencensus.common=com.google.api.client", "--add-exports=io.grpc/io.opencensus.trace.propagation=opencensus.contrib.http.util", "--add-exports=io.grpc/io.opencensus.trace=opencensus.contrib.http.util", "-p", "/app/lib", "-m", "no.ssb.dapla.metadata.distributor/no.ssb.dapla.metadata.distributor.Application"]
