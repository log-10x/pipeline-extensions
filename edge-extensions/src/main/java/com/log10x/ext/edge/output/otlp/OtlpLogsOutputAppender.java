package com.log10x.ext.edge.output.otlp;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.time.Instant;
import org.apache.logging.log4j.core.util.Booleans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
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
 * log4j2 appender that ships layout-rendered JSON events to an OpenTelemetry
 * Collector over OTLP/gRPC. Each rendered event is parsed back into a JSON
 * object; the field named by {@code bodyField} (default {@code body}) is
 * lifted into the OTLP {@code LogRecord.body}, every other top-level field
 * becomes a log-record attribute. The synthetic envelope tag carried by the
 * input (default {@code tag}) is dropped on the way out.
 */
@Plugin(name = OtlpLogsOutputAppender.PLUGIN_NAME,
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE,
        printObject = true)
public class OtlpLogsOutputAppender extends AbstractAppender {

    public static final String PLUGIN_NAME = "tenxOtlp";

    // Synthetic markers injected by OtlpLogsInputStream to carry the OTLP
    // wire shape that flat JSON loses (timestamps + resource partition).
    private static final String META_RESOURCE_KEYS = "_tenx_resource_keys";
    private static final String META_TIME = "_tenx_time";
    private static final String META_OBSERVED_TIME = "_tenx_observed_time";
    private static final String META_PREFIX = "_tenx_";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String host;
    private final int port;
    private final boolean insecure;
    private final String bodyField;
    private final String tagField;
    private final int batchSize;
    private final long flushIntervalMs;

    private final ConcurrentLinkedQueue<PendingRecord> buffer = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger bufferSize = new java.util.concurrent.atomic.AtomicInteger();

    private static final class PendingRecord {
        final LogRecord record;
        final List<KeyValue> resourceAttrs;
        PendingRecord(LogRecord record, List<KeyValue> resourceAttrs) {
            this.record = record;
            this.resourceAttrs = resourceAttrs;
        }
    }

    private ManagedChannel channel;
    private LogsServiceGrpc.LogsServiceStub stub;
    private ScheduledExecutorService flusher;

    // One observer per appender lifetime: gRPC calls it from a Netty thread,
    // there is no per-call state, and onError just logs. Avoids the anonymous
    // inner-class allocation every flush() incurred otherwise.
    private final StreamObserver<ExportLogsServiceResponse> exportObserver =
        new StreamObserver<ExportLogsServiceResponse>() {
            @Override public void onNext(ExportLogsServiceResponse r) { }
            @Override public void onError(Throwable t) {
                LOGGER.warn("tenxOtlp export error: {}", t.getMessage());
            }
            @Override public void onCompleted() { }
        };

    protected OtlpLogsOutputAppender(final String name,
                                     final Filter filter,
                                     final Layout<? extends Serializable> layout,
                                     final boolean ignoreExceptions,
                                     final String host,
                                     final int port,
                                     final boolean insecure,
                                     final String bodyField,
                                     final String tagField,
                                     final int batchSize,
                                     final long flushIntervalMs) {

        super(name, filter, layout != null ? layout : PatternLayout.createDefaultLayout(),
              ignoreExceptions, Property.EMPTY_ARRAY);

        this.host = host;
        this.port = port;
        this.insecure = insecure;
        this.bodyField = bodyField;
        this.tagField = tagField;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void start() {
        NettyChannelBuilder b = NettyChannelBuilder.forAddress(host, port);
        if (insecure) {
            b.usePlaintext();
        }
        this.channel = b.build();
        this.stub = LogsServiceGrpc.newStub(channel);

        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otlp-output-flusher");
            t.setDaemon(true);
            return t;
        });
        this.flusher.scheduleAtFixedRate(this::flush,
            flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        LOGGER.info("tenxOtlp appender '{}' connecting to {}:{} (insecure={})",
            getName(), host, port, insecure);

        super.start();
    }

    @Override
    public void append(LogEvent event) {

        byte[] bytes = getLayout().toByteArray(event);
        if (bytes == null || bytes.length == 0) {
            return;
        }

        // Find the first and last non-whitespace bytes without allocating a
        // String. UTF-8 whitespace at the envelope boundary is always ASCII.
        int start = 0;
        int end = bytes.length;
        while (start < end && bytes[start] <= ' ') {
            start++;
        }
        while (end > start && bytes[end - 1] <= ' ') {
            end--;
        }
        if (start == end) {
            return;
        }

        JsonNode tree = null;
        if (bytes[start] == '{') {
            try {
                JsonNode parsed = MAPPER.readTree(bytes, start, end - start);
                if (parsed.isObject()) {
                    tree = parsed;
                }
            } catch (Exception ignored) {
                // fall through to raw-text path
            }
        }

        LogRecord.Builder lr = LogRecord.newBuilder();
        Instant inst = event.getInstant();
        long fallbackNanos = inst.getEpochSecond() * 1_000_000_000L + inst.getNanoOfSecond();

        List<KeyValue> resourceAttrs = Collections.emptyList();

        if (tree != null) {
            Set<String> resourceKeySet = readResourceKeys(tree);
            boolean hasTime = setTimestamps(lr, tree, fallbackNanos);
            if (!hasTime) {
                lr.setTimeUnixNano(fallbackNanos);
                lr.setObservedTimeUnixNano(fallbackNanos);
            }

            if (!resourceKeySet.isEmpty()) {
                resourceAttrs = new ArrayList<>(resourceKeySet.size());
            }

            for (Map.Entry<String, JsonNode> e : tree.properties()) {
                String key = e.getKey();
                JsonNode val = e.getValue();

                if (key.startsWith(META_PREFIX)) {
                    continue;
                }
                if (key.equals(tagField)) {
                    continue;
                }
                if (key.equals(bodyField)) {
                    lr.setBody(otlpBodyFromShape(val));
                } else if (resourceKeySet.contains(key)) {
                    resourceAttrs.add(KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(jsonToAnyValue(val))
                        .build());
                } else {
                    lr.addAttributes(KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(jsonToAnyValue(val))
                        .build());
                }
            }
        } else {
            // Layout rendered a single non-JSON value (e.g. the compact
            // encoded form in optimize mode). Carry it verbatim in the body —
            // the String allocation lives only on this branch.
            lr.setTimeUnixNano(fallbackNanos);
            lr.setObservedTimeUnixNano(fallbackNanos);
            String text = new String(bytes, start, end - start, StandardCharsets.UTF_8);
            lr.setBody(AnyValue.newBuilder().setStringValue(text).build());
        }

        buffer.add(new PendingRecord(lr.build(), resourceAttrs));
        int size = bufferSize.incrementAndGet();
        if (size >= batchSize) {
            flush();
        }
    }

    private void flush() {

        if (bufferSize.get() == 0) {
            return;
        }

        List<PendingRecord> drained = new ArrayList<>();
        PendingRecord r;
        while ((r = buffer.poll()) != null) {
            drained.add(r);
            bufferSize.decrementAndGet();
            if (drained.size() >= 1024) {
                break;
            }
        }
        if (drained.isEmpty()) {
            return;
        }

        // Group records by resource fingerprint so each unique resource
        // becomes its own ResourceLogs entry (preserving the OTLP wire
        // distinction between resource and log-record attributes).
        LinkedHashMap<List<KeyValue>, List<LogRecord>> grouped = new LinkedHashMap<>();
        for (PendingRecord pr : drained) {
            grouped.computeIfAbsent(pr.resourceAttrs, k -> new ArrayList<>()).add(pr.record);
        }

        ExportLogsServiceRequest.Builder reqBuilder = ExportLogsServiceRequest.newBuilder();
        for (Map.Entry<List<KeyValue>, List<LogRecord>> entry : grouped.entrySet()) {
            ResourceLogs.Builder rlBuilder = ResourceLogs.newBuilder();
            if (!entry.getKey().isEmpty()) {
                Resource.Builder resBuilder = Resource.newBuilder();
                for (KeyValue kv : entry.getKey()) {
                    resBuilder.addAttributes(kv);
                }
                rlBuilder.setResource(resBuilder);
            }
            rlBuilder.addScopeLogs(ScopeLogs.newBuilder()
                .addAllLogRecords(entry.getValue())
                .build());
            reqBuilder.addResourceLogs(rlBuilder.build());
        }

        try {
            stub.export(reqBuilder.build(), exportObserver);
        } catch (Exception ex) {
            LOGGER.warn("tenxOtlp export submit failed: {}", ex.getMessage());
        }
    }

    private static Set<String> readResourceKeys(JsonNode tree) {
        JsonNode keysNode = tree.get(META_RESOURCE_KEYS);
        if (keysNode == null || !keysNode.isArray() || keysNode.size() == 0) {
            return Collections.emptySet();
        }
        Set<String> out = new HashSet<>(keysNode.size() * 2);
        for (JsonNode k : keysNode) {
            if (k.isTextual()) {
                out.add(k.asText());
            }
        }
        return out;
    }

    private static boolean setTimestamps(LogRecord.Builder lr, JsonNode tree, long fallbackNanos) {
        JsonNode t = tree.get(META_TIME);
        JsonNode obs = tree.get(META_OBSERVED_TIME);
        if (t == null && obs == null) {
            return false;
        }
        if (t != null && t.canConvertToLong()) {
            lr.setTimeUnixNano(t.asLong());
        }
        if (obs != null && obs.canConvertToLong()) {
            lr.setObservedTimeUnixNano(obs.asLong());
        } else if (t != null && t.canConvertToLong()) {
            lr.setObservedTimeUnixNano(t.asLong());
        } else {
            lr.setObservedTimeUnixNano(fallbackNanos);
        }
        return true;
    }

    private AnyValue otlpBodyFromShape(JsonNode val) {

        if (val.isObject()) {
            if (val.has("stringValue")) {
                return AnyValue.newBuilder().setStringValue(val.get("stringValue").asText("")).build();
            }
            if (val.has("intValue")) {
                return AnyValue.newBuilder().setIntValue(val.get("intValue").asLong()).build();
            }
            if (val.has("doubleValue")) {
                return AnyValue.newBuilder().setDoubleValue(val.get("doubleValue").asDouble()).build();
            }
            if (val.has("boolValue")) {
                return AnyValue.newBuilder().setBoolValue(val.get("boolValue").asBoolean()).build();
            }
            if (val.has("kvlistValue")) {
                JsonNode kv = val.get("kvlistValue");
                KeyValueList.Builder kb = KeyValueList.newBuilder();
                if (kv.isObject()) {
                    for (Map.Entry<String, JsonNode> e : kv.properties()) {
                        kb.addValues(KeyValue.newBuilder()
                            .setKey(e.getKey())
                            .setValue(jsonToAnyValue(e.getValue()))
                            .build());
                    }
                }
                return AnyValue.newBuilder().setKvlistValue(kb).build();
            }
            if (val.has("arrayValue")) {
                JsonNode av = val.get("arrayValue");
                ArrayValue.Builder ab = ArrayValue.newBuilder();
                if (av.isArray()) {
                    for (JsonNode n : av) {
                        ab.addValues(jsonToAnyValue(n));
                    }
                } else if (av.isObject() && av.has("values") && av.get("values").isArray()) {
                    for (JsonNode n : av.get("values")) {
                        ab.addValues(jsonToAnyValue(n));
                    }
                }
                return AnyValue.newBuilder().setArrayValue(ab).build();
            }
        }
        return jsonToAnyValue(val);
    }

    private AnyValue jsonToAnyValue(JsonNode val) {

        if (val == null || val.isNull()) {
            return AnyValue.newBuilder().build();
        }
        if (val.isTextual()) {
            return AnyValue.newBuilder().setStringValue(val.asText()).build();
        }
        if (val.isBoolean()) {
            return AnyValue.newBuilder().setBoolValue(val.asBoolean()).build();
        }
        if (val.isIntegralNumber()) {
            return AnyValue.newBuilder().setIntValue(val.asLong()).build();
        }
        if (val.isFloatingPointNumber()) {
            return AnyValue.newBuilder().setDoubleValue(val.asDouble()).build();
        }
        if (val.isArray()) {
            ArrayValue.Builder ab = ArrayValue.newBuilder();
            for (JsonNode n : val) {
                ab.addValues(jsonToAnyValue(n));
            }
            return AnyValue.newBuilder().setArrayValue(ab).build();
        }
        if (val.isObject()) {
            KeyValueList.Builder kb = KeyValueList.newBuilder();
            for (Map.Entry<String, JsonNode> e : val.properties()) {
                kb.addValues(KeyValue.newBuilder()
                    .setKey(e.getKey())
                    .setValue(jsonToAnyValue(e.getValue()))
                    .build());
            }
            return AnyValue.newBuilder().setKvlistValue(kb).build();
        }
        return AnyValue.newBuilder().setStringValue(val.asText()).build();
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {

        setStopping();
        try {
            if (flusher != null) {
                flusher.shutdownNow();
            }
            flush();
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(timeout, timeUnit);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("tenxOtlp stop error: {}", ex.getMessage());
        }
        super.stop(timeout, timeUnit, false);
        setStopped();
        return true;
    }

    @PluginFactory
    public static OtlpLogsOutputAppender createAppender(
            @PluginAttribute("name") final String name,
            @PluginAttribute(value = "host", defaultString = "127.0.0.1") final String host,
            @PluginAttribute(value = "port", defaultInt = 4317) final int port,
            @PluginAttribute(value = "insecure", defaultBoolean = true) final boolean insecure,
            @PluginAttribute(value = "bodyField", defaultString = "body") final String bodyField,
            @PluginAttribute(value = "tagField", defaultString = "tag") final String tagField,
            @PluginAttribute(value = "batchSize", defaultInt = 100) final int batchSize,
            @PluginAttribute(value = "flushIntervalMs", defaultLong = 200L) final long flushIntervalMs,
            @PluginAttribute("ignoreExceptions") final String ignoreExceptions,
            @PluginElement(Layout.ELEMENT_TYPE) Layout<? extends Serializable> layout,
            @PluginElement(Filter.ELEMENT_TYPE) final Filter filter) {

        if (name == null) {
            LOGGER.error("No name provided for tenxOtlp appender");
            return null;
        }

        return new OtlpLogsOutputAppender(
            name, filter, layout,
            Booleans.parseBoolean(ignoreExceptions, true),
            host, port, insecure,
            bodyField, tagField,
            batchSize, flushIntervalMs);
    }

    @Override
    public String toString() {
        return "OtlpLogsOutputAppender[name=" + getName()
            + ", host=" + host + ", port=" + port
            + ", insecure=" + insecure + "]";
    }
}
