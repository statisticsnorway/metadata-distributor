package no.ssb.helidon.application;

import com.google.protobuf.MessageOrBuilder;
import io.helidon.webserver.ServerRequest;
import io.opentracing.Span;
import io.opentracing.Tracer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class Tracing {

    public static <T extends MessageOrBuilder> T traceInputMessage(Span span, T message) {
        traceInputMessage(span, message.toString());
        return message;
    }

    public static <T extends MessageOrBuilder> T traceOutputMessage(Span span, T message) {
        traceOutputMessage(span, message.toString());
        return message;
    }

    public static void traceInputMessage(Span span, String message) {
        span.log(Map.of("event", "debug-input", "data", message));
    }

    public static void traceOutputMessage(Span span, String message) {
        span.log(Map.of("event", "debug-output", "data", message));
    }

    public static void logError(Span span, Throwable e) {
        logError(span, e, "error");
    }

    public static void logError(Span span, Throwable e, String event) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        span.log(Map.of("event", event, "message", ofNullable(e.getMessage()).orElse(""), "stacktrace", stringWriter.toString()));
    }

    public static void logError(Span span, Throwable e, String event, String... fields) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            map.put(fields[i], fields[i + 1]);
        }
        logError(span, e, event, map);
    }

    public static void logError(Span span, Throwable e, String event, Map<String, String> fields) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        Map<String, String> map = new LinkedHashMap<>();
        map.put("event", event);
        map.put("message", e.getMessage());
        map.put("stacktrace", stringWriter.toString());
        map.putAll(fields);
        span.log(map);
    }

    public static void restoreTracingContext(Tracer tracer, Span span) {
        tracer.scopeManager().activate(span);
    }

    public static Optional<Span> spanFromHttp(ServerRequest request, String operationName) {
        return request.spanContext()
                .map(ctx -> request.tracer()
                        .buildSpan(operationName)
                        .asChildOf(ctx)
                        .start()
                );
    }
}
