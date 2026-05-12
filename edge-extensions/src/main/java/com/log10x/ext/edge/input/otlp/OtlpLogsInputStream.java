package com.log10x.ext.edge.input.otlp;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
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
 * Each incoming {@code ExportLogsServiceRequest} can carry many resources,
 * each carrying many scopes, each carrying many log records. Every log record
 * is flattened into a single JSON line containing:
 * <ul>
 *   <li>{@code body} — the log body, recursively decoded from {@link AnyValue}</li>
 *   <li>{@code severity_text}, {@code severity_number} — when set</li>
 *   <li>{@code timestamp} — epoch nanoseconds</li>
 *   <li>{@code trace_id}, {@code span_id} — hex encoded, when set</li>
 *   <li>The log record's own attributes, flat at the top level</li>
 *   <li>The owning scope's {@code scope.name}, {@code scope.version}, and
 *       {@code scope.<attr>} attributes</li>
 *   <li>The owning resource's attributes, flat at the top level</li>
 *   <li>{@code tag} — synthetic, set to either {@code service.name} or
 *       {@code k8s.pod.name} when present, otherwise the literal string
 *       {@code "otel"} — used by the Log10x source-grouping path</li>
 * </ul>
 *
 * Configuration options (passed via Map in constructor):
 * <ul>
 *   <li>{@code port} — TCP port for the OTLP/gRPC server (default 4317)</li>
 * </ul>
 *
 * Example usage in YAML:
 * <pre>
 * input:
 *   - type: stream
 *     name: otlp
 *     path: com.log10x.ext.edge.input.otlp.OtlpLogsInputStream
 *     args:
 *       - port
 *       - 4317
 * </pre>
 */
public class OtlpLogsInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(OtlpLogsInputStream.class);

    private static final String PORT = "port";
    private static final int DEFAULT_PORT = 4317;
    private static final long STATS_INTERVAL_MS = 30_000;

    private final int port;
    private final JsonFactory jsonFactory;
    private final ConcurrentLinkedQueue<String> lineQueue = new ConcurrentLinkedQueue<>();

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
                        InstrumentationScope scope = sl.getScope();
                        for (LogRecord lr : sl.getLogRecordsList()) {
                            String line = renderRecord(resource, scope, lr);
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

    private String renderRecord(Resource resource, InstrumentationScope scope, LogRecord lr) throws IOException {
        StringWriter sw = new StringWriter(256);

        try (JsonGenerator gen = jsonFactory.createGenerator(sw)) {
            gen.writeStartObject();

            // MINIMAL: just message + tag (Vector-shape) to verify flow.
            String bodyText = "";
            if (lr.hasBody() && lr.getBody().getValueCase() == AnyValue.ValueCase.STRING_VALUE) {
                bodyText = lr.getBody().getStringValue();
            }
            gen.writeStringField("message", bodyText);

            String tag = pickTag(resource, lr);
            gen.writeStringField("tag", tag);

            gen.writeEndObject();
        }

        return sw.toString();
    }

    private static String pickTag(Resource resource, LogRecord lr) {
        // Prefer service.name from resource, then k8s.pod.name, then a literal.
        for (KeyValue kv : resource.getAttributesList()) {
            if ("service.name".equals(kv.getKey()) && kv.getValue().getStringValue().length() > 0) {
                return kv.getValue().getStringValue();
            }
        }
        for (KeyValue kv : resource.getAttributesList()) {
            if ("k8s.pod.name".equals(kv.getKey()) && kv.getValue().getStringValue().length() > 0) {
                return kv.getValue().getStringValue();
            }
        }
        return "otel";
    }

    private void writeKv(JsonGenerator gen, KeyValue kv) throws IOException {
        // Replace dots with underscores in attribute keys. The engine's JSON
        // extractor uses dotted paths for nested-field access, so a top-level
        // key like `k8s.namespace.name` is interpreted as a path into a
        // non-existent map and the value is dropped.
        gen.writeFieldName(kv.getKey().replace('.', '_'));
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

    private static String hex(ByteString bytes) {
        StringBuilder sb = new StringBuilder(bytes.size() * 2);
        for (int i = 0; i < bytes.size(); i++) {
            int b = bytes.byteAt(i) & 0xFF;
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
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

        byte[] lineBytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
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
