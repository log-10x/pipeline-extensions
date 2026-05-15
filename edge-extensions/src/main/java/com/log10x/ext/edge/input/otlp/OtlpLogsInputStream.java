package com.log10x.ext.edge.input.otlp;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * An input stream that listens for OpenTelemetry Protocol (OTLP) log records
 * over gRPC and emits newline-delimited JSON records for the Log10x pipeline
 * to consume.
 *
 * Each log record inside an incoming {@code ExportLogsServiceRequest} is
 * emitted as a single JSON line containing:
 * <ul>
 *   <li>{@code body} — the log body, recursively decoded from {@link AnyValue}
 *       in its OTLP shape (e.g. {@code {"stringValue":"…"}})</li>
 *   <li>The log record's attributes, flat at the top level (keys unchanged)</li>
 *   <li>The owning resource's attributes, flat at the top level (keys unchanged)</li>
 *   <li>{@code tag} — synthetic envelope-level tag, set to {@code service.name}
 *       if present, otherwise {@code k8s.pod.name}, otherwise the literal
 *       {@code "otel"} — used by the Forward output as the wire tag and by
 *       the engine's {@code sourcePattern} as the event source</li>
 * </ul>
 *
 * Field-content interpretation (which key carries the log line, etc.) is
 * the engine's responsibility — see the {@code extractor} block in
 * {@code input/stream.yaml}.
 */
public class OtlpLogsInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(OtlpLogsInputStream.class);

    private static final String PORT = "port";
    private static final int DEFAULT_PORT = 4317;
    private static final long STATS_INTERVAL_MS = 30_000;

    private final int port;
    private final JsonFactory jsonFactory;
    private final ConcurrentLinkedQueue<String> lineQueue = new ConcurrentLinkedQueue<>();

    // Per-thread scratch buffer reused across renderRecord calls. gRPC drives
    // export() from a small pool of Netty handler threads, so per-record
    // allocation of the 512-byte StringWriter becomes hot when records flow.
    // The ThreadLocal is rooted on its owning thread, so it dies with the
    // thread — no global leak. StringBuffer synchronization inside the writer
    // is uncontended (single thread per ThreadLocal slot) so JIT elides it.
    private static final ThreadLocal<StringWriter> RENDER_BUF =
        ThreadLocal.withInitial(() -> new StringWriter(512));

    private Server server;
    private byte[] pendingBytes;
    private int pendingOffset;
    private boolean closed;

    private final ScheduledExecutorService statsTicker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "otlp-input-stats");
        t.setDaemon(true);
        return t;
    });

    private final AtomicLong recordsDecoded = new AtomicLong();
    private final AtomicLong recordsConsumed = new AtomicLong();
    private final AtomicLong bytesEmitted = new AtomicLong();
    private long lastRecordsDecoded = 0;
    private long lastRecordsConsumed = 0;

    public OtlpLogsInputStream(Map<String, Object> args) throws IOException {

        if (args == null) {
            throw new IllegalArgumentException("expected port in args, received: null");
        }

        Object portObj = args.get(PORT);
        this.port = (portObj == null || String.valueOf(portObj).isEmpty())
            ? DEFAULT_PORT
            : Integer.parseInt(String.valueOf(portObj));

        this.jsonFactory = new JsonFactory();
        this.closed = false;

        logger.info("initializing OTLP/gRPC logs input on TCP port: {}", port);

        startServer();
        startStatsTicker();
    }

    private void startServer() throws IOException {
        this.server = NettyServerBuilder.forPort(port)
            .addService(new LogsServiceImpl())
            .build()
            .start();
        logger.info("OTLP/gRPC logs server listening on TCP port: {}", port);
    }

    private void startStatsTicker() {
        statsTicker.scheduleAtFixedRate(this::logStats,
            STATS_INTERVAL_MS, STATS_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void logStats() {
        long decoded = recordsDecoded.get();
        long consumed = recordsConsumed.get();
        long bytes = bytesEmitted.get();
        long deltaDecoded = decoded - lastRecordsDecoded;
        long deltaConsumed = consumed - lastRecordsConsumed;
        long pending = lineQueue.size();
        logger.debug("otlp-input stats: decoded={} consumed={} delta(decoded={}, consumed={}) pending={} bytes={}",
            decoded, consumed, deltaDecoded, deltaConsumed, pending, bytes);
        lastRecordsDecoded = decoded;
        lastRecordsConsumed = consumed;
    }

    // ── Polling readLine for the engine ────────────────────────────────────────

    public String readLine() throws IOException {
        if (closed) {
            return null;
        }

        // Spin-wait with backoff. The engine reads in a dedicated thread, so
        // a brief sleep when empty is fine — avoids CPU burn while keeping
        // latency low when records arrive.
        while (true) {
            String queued = lineQueue.poll();
            if (queued != null) {
                recordsConsumed.incrementAndGet();
                return queued;
            }
            if (closed) {
                return null;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    // ── gRPC service impl ──────────────────────────────────────────────────────

    private final class LogsServiceImpl extends LogsServiceGrpc.LogsServiceImplBase {

        @Override
        public void export(ExportLogsServiceRequest request,
                           StreamObserver<ExportLogsServiceResponse> responseObserver) {
            try {
                int decoded = 0;
                for (ResourceLogs rl : request.getResourceLogsList()) {
                    Resource resource = rl.getResource();
                    for (ScopeLogs sl : rl.getScopeLogsList()) {
                        for (LogRecord lr : sl.getLogRecordsList()) {
                            String line = renderRecord(resource, lr);
                            lineQueue.add(line);
                            decoded++;
                        }
                    }
                }
                if (decoded > 0) {
                    recordsDecoded.addAndGet(decoded);
                }

                responseObserver.onNext(ExportLogsServiceResponse.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.warn("error decoding OTLP logs request", e);
                responseObserver.onError(e);
            }
        }
    }

    // ── OTLP → JSON flattening ─────────────────────────────────────────────────

    private String renderRecord(Resource resource, LogRecord lr) throws IOException {
        StringWriter sw = RENDER_BUF.get();
        sw.getBuffer().setLength(0);

        try (JsonGenerator gen = jsonFactory.createGenerator(sw)) {
            gen.writeStartObject();

            // Body — for plain-text log lines (stringValue) the body is
            // emitted as a top-level string field so the engine's outer-text
            // accessor sees it as a flat capture (needed by `encode()` to
            // preserve the surrounding envelope in optimize mode). For
            // non-string bodies the value is emitted in its OTLP AnyValue
            // shape so it stays losslessly addressable downstream.
            if (lr.hasBody()) {
                AnyValue body = lr.getBody();
                if (body.getValueCase() == AnyValue.ValueCase.STRING_VALUE) {
                    gen.writeStringField("body", body.getStringValue());
                } else {
                    gen.writeFieldName("body");
                    writeAnyValue(gen, body);
                }
            }

            // Log-record attributes (flat)
            for (KeyValue kv : lr.getAttributesList()) {
                writeKv(gen, kv);
            }

            // Resource attributes (flat)
            for (KeyValue kv : resource.getAttributesList()) {
                writeKv(gen, kv);
            }

            // Synthetic envelope-level tag — Forward output uses this as the
            // wire tag, and the engine's `sourcePattern` extracts it as the
            // event source.
            gen.writeStringField("tag", pickTag(resource));

            // Synthetic markers carrying the OTLP shape that flat JSON loses:
            // which top-level keys belong on `Resource` vs the log record, plus
            // the original OTLP timestamps (the engine's logEvent time is reset
            // on the way through the pipeline). The `_tenx_` prefix keeps them
            // from colliding with user fields and lets the output appender
            // strip them on the way out.
            if (resource.getAttributesCount() > 0) {
                gen.writeArrayFieldStart("_tenx_resource_keys");
                for (KeyValue kv : resource.getAttributesList()) {
                    gen.writeString(kv.getKey());
                }
                gen.writeEndArray();
            }
            gen.writeNumberField("_tenx_time", lr.getTimeUnixNano());
            gen.writeNumberField("_tenx_observed_time", lr.getObservedTimeUnixNano());

            gen.writeEndObject();
        }
        // Trailing newline appended after the generator closes — keeps a
        // single allocation downstream, since read() can hand these bytes
        // straight to the engine without an extra String + getBytes() round
        // trip just to append \n.
        sw.append('\n');
        return sw.toString();
    }

    private static String pickTag(Resource resource) {
        // Single pass: prefer service.name, fall back to k8s.pod.name.
        String podName = null;
        for (KeyValue kv : resource.getAttributesList()) {
            String key = kv.getKey();
            if ("service.name".equals(key)) {
                String v = kv.getValue().getStringValue();
                if (!v.isEmpty()) {
                    return v;
                }
            } else if (podName == null && "k8s.pod.name".equals(key)) {
                String v = kv.getValue().getStringValue();
                if (!v.isEmpty()) {
                    podName = v;
                }
            }
        }
        return podName != null ? podName : "otel";
    }

    private void writeKv(JsonGenerator gen, KeyValue kv) throws IOException {
        gen.writeFieldName(kv.getKey());
        writeAnyValue(gen, kv.getValue());
    }

    private void writeAnyValue(JsonGenerator gen, AnyValue v) throws IOException {
        switch (v.getValueCase()) {
            case STRING_VALUE:
                gen.writeString(v.getStringValue());
                break;
            case BOOL_VALUE:
                gen.writeBoolean(v.getBoolValue());
                break;
            case INT_VALUE:
                gen.writeNumber(v.getIntValue());
                break;
            case DOUBLE_VALUE:
                gen.writeNumber(v.getDoubleValue());
                break;
            case BYTES_VALUE:
                gen.writeBinary(v.getBytesValue().toByteArray());
                break;
            case ARRAY_VALUE:
                gen.writeStartArray();
                ArrayValue av = v.getArrayValue();
                for (AnyValue item : av.getValuesList()) {
                    writeAnyValue(gen, item);
                }
                gen.writeEndArray();
                break;
            case KVLIST_VALUE:
                gen.writeStartObject();
                KeyValueList kvl = v.getKvlistValue();
                for (KeyValue kv : kvl.getValuesList()) {
                    writeKv(gen, kv);
                }
                gen.writeEndObject();
                break;
            case VALUE_NOT_SET:
            default:
                gen.writeNull();
                break;
        }
    }

    // ── InputStream interface ──────────────────────────────────────────────────

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int result = read(b, 0, 1);
        return result == -1 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            return -1;
        }

        if (pendingBytes != null) {
            int available = pendingBytes.length - pendingOffset;
            int copyLen = Math.min(len, available);
            System.arraycopy(pendingBytes, pendingOffset, b, off, copyLen);
            pendingOffset += copyLen;
            if (pendingOffset >= pendingBytes.length) {
                pendingBytes = null;
                pendingOffset = 0;
            }
            bytesEmitted.addAndGet(copyLen);
            return copyLen;
        }

        String line = readLine();
        if (line == null) {
            return -1;
        }

        // renderRecord already appended '\n' before returning, so a single
        // UTF-8 encode is all we need here — no extra String concat.
        byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
        int copyLen = Math.min(len, lineBytes.length);
        System.arraycopy(lineBytes, 0, b, off, copyLen);

        if (copyLen < lineBytes.length) {
            pendingBytes = lineBytes;
            pendingOffset = copyLen;
        }

        bytesEmitted.addAndGet(copyLen);
        return copyLen;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        logger.info("closing OTLP/gRPC logs input stream on port: {}", port);

        statsTicker.shutdownNow();

        if (server != null) {
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
    }

    @Override
    public String toString() {
        return "OtlpLogsInputStream[port=" + port + "]";
    }
}
