package com.log10x.ext.edge.input.forward;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * An input stream that reads the Fluent Forward protocol from a Unix domain socket
 * and emits newline-delimited JSON records for the Log10x pipeline to consume.
 *
 * Supports all standard Forward protocol modes:
 * <ul>
 *   <li><b>Message</b>: {@code [tag, time, record]} - single event</li>
 *   <li><b>Forward</b>: {@code [tag, [[time, record], ...]]} - batch of events</li>
 *   <li><b>PackedForward</b>: {@code [tag, packed_entries_bin]} - packed binary entries</li>
 * </ul>
 *
 * Each decoded record is emitted as a JSON line with the tag injected,
 * matching the format expected by the Log10x input reader:
 * <pre>{"tag":"kube.some.tag","log":"...","kubernetes":{...}}</pre>
 *
 * Configuration options (passed via Map in constructor):
 * <ul>
 *   <li>{@code path}: Unix socket path (required)</li>
 * </ul>
 *
 * Example usage in YAML:
 * <pre>
 * input:
 *   - type: custom
 *     class: com.log10x.ext.edge.input.forward.ForwardProtocolInputStream
 *     options:
 *       path: /tenx-sockets/tenx-reporter.sock
 * </pre>
 */
public class ForwardProtocolInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(ForwardProtocolInputStream.class);

    private static final String PATH = "path";
    private static final String PORT = "port";

    /** Fluent EventTime extension type code. */
    private static final byte EVENT_TIME_EXT_TYPE = 0;

    private final String socketPath;
    private final int port;
    private final JsonFactory jsonFactory;

    private ServerSocketChannel server;
    private SocketChannel client;
    private MessageUnpacker unpacker;

    /** Queue of decoded JSON lines ready to be returned by {@link #readLine()}. */
    private final Queue<String> lineQueue = new ArrayDeque<>();

    /** Overflow bytes from a JSON line that didn't fit in the previous {@code read()} call. */
    private byte[] pendingBytes;
    private int pendingOffset;

    private boolean closed;

    /**
     * Constructor invoked by the Log10x runtime.
     *
     * @param args Map containing {@code path} (required)
     * @throws IOException if socket creation fails
     */
    public ForwardProtocolInputStream(Map<String, Object> args) throws IOException {

        if (args == null) {
            throw new IllegalArgumentException("expected socket path or port in args, received: null");
        }

        boolean hasPath = args.containsKey(PATH) && args.get(PATH) != null
                && !String.valueOf(args.get(PATH)).isEmpty();

        boolean hasPort = args.containsKey(PORT) && args.get(PORT) != null
                && !String.valueOf(args.get(PORT)).isEmpty();

        if (!hasPath && !hasPort) {
            throw new IllegalArgumentException("expected socket path or port in args, received: " + args);
        }

        // Unix socket path takes precedence over TCP port when specified
        this.socketPath = hasPath ? String.valueOf(args.get(PATH)) : null;
        this.port = hasPath ? -1 : Integer.parseInt(String.valueOf(args.get(PORT)));
        this.jsonFactory = new JsonFactory();
        this.closed = false;

        if (socketPath != null) {
            logger.info("initializing Forward protocol input stream at: {}", socketPath);
        } else {
            logger.info("initializing Forward protocol input stream on TCP port: {}", port);
        }

        open();
    }

    private void open() throws IOException {
        if (socketPath == null) {
            server = ServerSocketChannel.open();
            server.bind(new InetSocketAddress(port));
            logger.info("Forward protocol server listening on TCP port: {}", port);
        } else {
            Path path = Path.of(socketPath);

            Files.deleteIfExists(path);

            Path parent = path.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            server.bind(UnixDomainSocketAddress.of(path));
            logger.info("Forward protocol server listening on: {}", socketPath);
        }
    }

    private void acceptClient() throws IOException {
        if (client == null || !client.isOpen()) {
            logger.debug("waiting for client connection on: {}", socketPath);
            client = server.accept();
            unpacker = MessagePack.newDefaultUnpacker(Channels.newInputStream(client));
            logger.info("client connected to Forward protocol socket: {}", socketPath);
        }
    }

    /**
     * Reads the next decoded JSON line from the forward protocol stream.
     * Blocks until a line is available or the stream is closed.
     */
    public String readLine() throws IOException {
        if (closed) {
            return null;
        }

        while (true) {
            String queued = lineQueue.poll();
            if (queued != null) {
                return queued;
            }

            try {
                acceptClient();
                decodeNextMessage();
            } catch (MessageInsufficientBufferException | EOFException e) {
                logger.info("client disconnected, waiting for new connection");
                closeClient();
            }
        }
    }

    // ── Forward protocol decoding ──────────────────────────────────────────────

    /**
     * Reads and decodes the next forward protocol message, queuing one or more
     * JSON lines into {@link #lineQueue}.
     */
    private void decodeNextMessage() throws IOException {
        if (!unpacker.hasNext()) {
            throw new EOFException();
        }

        int arraySize = unpacker.unpackArrayHeader();

        if (arraySize < 2) {
            logger.warn("unexpected forward protocol array size: {}, skipping", arraySize);
            for (int i = 0; i < arraySize; i++) {
                unpacker.skipValue();
            }
            return;
        }

        String tag = unpacker.unpackString();

        MessageFormat nextFormat = unpacker.getNextFormat();
        ValueType nextType = nextFormat.getValueType();

        if (nextType == ValueType.ARRAY) {
            // Forward mode: [tag, [[time, record], [time, record], ...]]
            decodeForwardEntries(tag);
        } else if (nextType == ValueType.BINARY) {
            // PackedForward mode: [tag, packed_entries_bin, options?]
            decodePackedForward(tag);
        } else if (nextType == ValueType.INTEGER || nextType == ValueType.EXTENSION) {
            // Message mode: [tag, time, record, options?]
            unpacker.skipValue(); // skip time
            Value record = unpacker.unpackValue();
            queueRecord(tag, record);
        } else {
            logger.warn("unexpected forward protocol element type: {}, skipping message", nextFormat);
            for (int i = 1; i < arraySize; i++) {
                unpacker.skipValue();
            }
            return;
        }

        // Skip trailing options if present
        int consumed = (nextType == ValueType.INTEGER || nextType == ValueType.EXTENSION) ? 3 : 2;
        for (int i = consumed; i < arraySize; i++) {
            unpacker.skipValue();
        }
    }

    /**
     * Forward mode: entries is an array of [time, record] pairs.
     */
    private void decodeForwardEntries(String tag) throws IOException {
        int entryCount = unpacker.unpackArrayHeader();

        for (int i = 0; i < entryCount; i++) {
            int pairSize = unpacker.unpackArrayHeader();
            unpacker.skipValue(); // skip time
            Value record = unpacker.unpackValue();
            // skip any extra fields in the pair
            for (int j = 2; j < pairSize; j++) {
                unpacker.skipValue();
            }
            queueRecord(tag, record);
        }
    }

    /**
     * PackedForward mode: entries are packed as a binary blob of concatenated
     * msgpack [time, record] pairs.
     */
    private void decodePackedForward(String tag) throws IOException {
        int binaryLength = unpacker.unpackBinaryHeader();
        byte[] packed = unpacker.readPayload(binaryLength);

        try (MessageUnpacker entryUnpacker = MessagePack.newDefaultUnpacker(packed)) {
            while (entryUnpacker.hasNext()) {
                int pairSize = entryUnpacker.unpackArrayHeader();
                entryUnpacker.skipValue(); // skip time
                Value record = entryUnpacker.unpackValue();
                for (int j = 2; j < pairSize; j++) {
                    entryUnpacker.skipValue();
                }
                queueRecord(tag, record);
            }
        }
    }

    // ── Record to JSON conversion ──────────────────────────────────────────────

    /**
     * Converts a msgpack record value to JSON, injects the tag, and queues it.
     */
    private void queueRecord(String tag, Value record) throws IOException {
        if (!record.isMapValue()) {
            logger.debug("skipping non-map record: {}", record.getValueType());
            return;
        }

        StringWriter sw = new StringWriter(512);

        try (JsonGenerator gen = jsonFactory.createGenerator(sw)) {
            gen.writeStartObject();
            gen.writeStringField("tag", tag);
            writeMapEntries(gen, record.asMapValue());
            gen.writeEndObject();
        }

        lineQueue.add(sw.toString());
    }

    /**
     * Writes all entries of a msgpack map into the current JSON object.
     * Does not write start/end object markers.
     */
    private void writeMapEntries(JsonGenerator gen, MapValue map) throws IOException {
        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            Value keyVal = entry.getKey();
            String key = keyVal.isStringValue()
                ? keyVal.asStringValue().asString()
                : keyVal.toString();
            gen.writeFieldName(key);
            writeValue(gen, entry.getValue());
        }
    }

    /**
     * Recursively writes a msgpack value as JSON.
     */
    private void writeValue(JsonGenerator gen, Value value) throws IOException {
        switch (value.getValueType()) {
            case NIL:
                gen.writeNull();
                break;

            case BOOLEAN:
                gen.writeBoolean(value.asBooleanValue().getBoolean());
                break;

            case INTEGER:
                IntegerValue iv = value.asIntegerValue();
                if (iv.isInLongRange()) {
                    gen.writeNumber(iv.asLong());
                } else {
                    gen.writeNumber(iv.asBigInteger());
                }
                break;

            case FLOAT:
                gen.writeNumber(value.asFloatValue().toDouble());
                break;

            case STRING:
                gen.writeString(value.asStringValue().asString());
                break;

            case BINARY:
                gen.writeBinary(value.asBinaryValue().asByteArray());
                break;

            case ARRAY:
                gen.writeStartArray();
                for (Value item : value.asArrayValue()) {
                    writeValue(gen, item);
                }
                gen.writeEndArray();
                break;

            case MAP:
                gen.writeStartObject();
                writeMapEntries(gen, value.asMapValue());
                gen.writeEndObject();
                break;

            case EXTENSION:
                writeExtensionValue(gen, value.asExtensionValue());
                break;

            default:
                gen.writeNull();
                break;
        }
    }

    /**
     * Handles msgpack extension types. Fluent EventTime (type 0) is decoded
     * to epoch seconds; other extension types are written as null.
     */
    private static void writeExtensionValue(JsonGenerator gen, ExtensionValue ext) throws IOException {
        if (ext.getType() == EVENT_TIME_EXT_TYPE && ext.getData().length == 8) {
            byte[] data = ext.getData();
            long seconds = ((long) (data[0] & 0xFF) << 24)
                         | ((long) (data[1] & 0xFF) << 16)
                         | ((long) (data[2] & 0xFF) << 8)
                         | (data[3] & 0xFF);
            gen.writeNumber(seconds);
        } else {
            gen.writeNull();
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

        // Serve leftover bytes from a previous line that didn't fit
        if (pendingBytes != null) {
            int available = pendingBytes.length - pendingOffset;
            int copyLen = Math.min(len, available);
            System.arraycopy(pendingBytes, pendingOffset, b, off, copyLen);
            pendingOffset += copyLen;
            if (pendingOffset >= pendingBytes.length) {
                pendingBytes = null;
                pendingOffset = 0;
            }
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

        return copyLen;
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    private void closeClient() {
        try {
            if (unpacker != null) unpacker.close();
        } catch (Exception e) {
            logger.debug("error closing unpacker", e);
        }

        try {
            if (client != null) client.close();
        } catch (Exception e) {
            logger.debug("error closing client channel", e);
        }

        unpacker = null;
        client = null;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        logger.info("closing Forward protocol input stream: {}", socketPath);

        closeClient();

        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException e) {
            logger.warn("error closing server channel", e);
        }

        if (socketPath != null) {
            try {
                Files.deleteIfExists(Path.of(socketPath));
            } catch (IOException e) {
                logger.warn("error deleting socket file", e);
            }
        }
    }

    @Override
    public String toString() {
        return port > 0
            ? "ForwardProtocolInputStream[port=" + port + "]"
            : "ForwardProtocolInputStream[path=" + socketPath + "]";
    }
}
