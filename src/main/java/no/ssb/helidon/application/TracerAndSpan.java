package no.ssb.helidon.application;

import io.opentracing.Span;
import io.opentracing.Tracer;

public class TracerAndSpan {
    final Tracer tracer;
    final Span span;

    TracerAndSpan(Tracer tracer, Span span) {
        this.tracer = tracer;
        this.span = span;
    }

    public Tracer tracer() {
        return tracer;
    }

    public Span span() {
        return span;
    }
}
