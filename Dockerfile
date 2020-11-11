FROM eu.gcr.io/prod-bip/alpine-jdk15-buildtools:master-7744b1c6a23129ceaace641d6d76d0a742440b58 as build

#
# Build stripped JVM
#
RUN ["jlink", "--strip-java-debug-attributes", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
 "--add-modules", "java.base,java.management,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki,jdk.jcmd,jdk.jartool,jdk.jdi,jdk.jfr"]

#
# Build Application image
#
FROM alpine:latest

RUN apk --no-cache add curl tar gzip nano jq

#
# Resources from build image
#
COPY --from=build /linked /jdk/
COPY --from=build /jdk/bin/jar /jdk/bin/jcmd /jdk/bin/jdb /jdk/bin/jfr /jdk/bin/jinfo /jdk/bin/jmap /jdk/bin/jps /jdk/bin/jstack /jdk/bin/jstat /jdk/bin/
COPY run.sh /app/
COPY target/libs /app/lib/
COPY target/metadata-distributor-*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/
COPY src/test/resources/metadata-verifier_keystore.p12 /app/secret/

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10160
EXPOSE 10168

#ENV JAVA_MODULE_SYSTEM_ENABLED=true
CMD ["./run.sh"]
