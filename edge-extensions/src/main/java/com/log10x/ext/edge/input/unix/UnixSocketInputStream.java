package com.log10x.ext.edge.input.unix;

import java.io.IOException;
import java.io.InputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An input stream that reads line-delimited data from a Unix domain socket.
 *
 * Optionally parses RFC5424 syslog format and extracts only the MSG field,
 * which is useful for receiving logs from OpenTelemetry Collector's syslog exporter.
 *
 * Configuration options (passed via Map in constructor):
 * - path: Unix socket path (required)
 * - syslog: If true, parse RFC5424 and extract MSG field only (optional, default: false)
 *
 * Example usage in YAML:
 * <pre>
 * input:
 *   - type: custom
 *     class: com.log10x.ext.edge.input.unix.UnixSocketInputStream
 *     options:
 *       path: /tmp/tenx-input.sock
 *       syslog: true
 * </pre>
 */
public class UnixSocketInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(UnixSocketInputStream.class);

    private static final String PATH = "path";
    private static final String SYSLOG = "syslog";

    private static final int BUFFER_SIZE = 8192;
    private static final byte NEWLINE = '\n';

    private final String socketPath;
    private final boolean parseSyslog;

    private ServerSocketChannel server;
    private SocketChannel client;
    private ByteBuffer readBuffer;
    private ByteBuffer lineBuffer;
    private int lineBufferPos;
    private boolean closed;

    /**
     * Constructor invoked by the Log10x runtime.
     *
     * @param args Map of arguments containing 'path' (required) and 'syslog' (optional)
     * @throws IOException if socket creation fails
     */
    public UnixSocketInputStream(Map<String, Object> args) throws IOException {

        if (args == null || !args.containsKey(PATH)) {
            throw new IllegalArgumentException("expected socket path in args, received: " + args);
        }

        this.socketPath = String.valueOf(args.get(PATH));
        this.parseSyslog = Boolean.TRUE.equals(args.get(SYSLOG)) ||
                           "true".equalsIgnoreCase(String.valueOf(args.get(SYSLOG)));

        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.readBuffer.flip(); // Start empty
        this.lineBuffer = ByteBuffer.allocate(BUFFER_SIZE * 4); // Allow for long lines
        this.lineBufferPos = 0;
        this.closed = false;

        logger.info("initializing Unix socket input stream at: {} (syslog parsing: {})",
            socketPath, parseSyslog);

        open();
    }

    private void open() throws IOException {
        Path path = Path.of(socketPath);

        // Clean up stale socket file
        Files.deleteIfExists(path);

        // Ensure parent directory exists
        Path parent = path.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        server.bind(UnixDomainSocketAddress.of(path));

        logger.info("Unix socket server listening on: {}", socketPath);
    }

    private void acceptClient() throws IOException {
        if (client == null || !client.isOpen()) {
            logger.debug("waiting for client connection on: {}", socketPath);
            client = server.accept();
            logger.info("client connected to Unix socket: {}", socketPath);
        }
    }

    /**
     * Reads a single line from the socket, optionally parsing syslog format.
     * This is the main method used by the pipeline.
     */
    public String readLine() throws IOException {
        if (closed) {
            return null;
        }

        acceptClient();

        while (true) {
            // Try to find a newline in the line buffer
            for (int i = 0; i < lineBufferPos; i++) {
                if (lineBuffer.get(i) == NEWLINE) {
                    // Extract the line
                    byte[] lineBytes = new byte[i];
                    lineBuffer.position(0);
                    lineBuffer.get(lineBytes, 0, i);

                    // Compact the remaining data
                    int remaining = lineBufferPos - i - 1;
                    if (remaining > 0) {
                        lineBuffer.position(i + 1);
                        byte[] temp = new byte[remaining];
                        lineBuffer.get(temp, 0, remaining);
                        lineBuffer.clear();
                        lineBuffer.put(temp);
                    } else {
                        lineBuffer.clear();
                    }
                    lineBufferPos = remaining;

                    String line = new String(lineBytes, StandardCharsets.UTF_8);

                    // If syslog mode, extract MSG field only
                    if (parseSyslog && !line.isEmpty()) {
                        return extractSyslogMessage(line);
                    }

                    return line;
                }
            }

            // Need more data - read from socket
            if (!readBuffer.hasRemaining()) {
                readBuffer.clear();
                int bytesRead = client.read(readBuffer);

                if (bytesRead == -1) {
                    // Client disconnected
                    logger.info("client disconnected, waiting for new connection");
                    client.close();
                    client = null;

                    // Return any remaining data as final line
                    if (lineBufferPos > 0) {
                        byte[] lineBytes = new byte[lineBufferPos];
                        lineBuffer.position(0);
                        lineBuffer.get(lineBytes, 0, lineBufferPos);
                        lineBufferPos = 0;
                        lineBuffer.clear();

                        String line = new String(lineBytes, StandardCharsets.UTF_8);
                        if (parseSyslog && !line.isEmpty()) {
                            return extractSyslogMessage(line);
                        }
                        return line;
                    }

                    // Wait for new client
                    acceptClient();
                    continue;
                }

                if (bytesRead == 0) {
                    continue;
                }

                readBuffer.flip();
            }

            // Copy data to line buffer, growing if needed
            while (readBuffer.hasRemaining()) {
                if (lineBufferPos >= lineBuffer.capacity()) {
                    // Grow line buffer
                    ByteBuffer newBuffer = ByteBuffer.allocate(lineBuffer.capacity() * 2);
                    lineBuffer.position(0);
                    lineBuffer.limit(lineBufferPos);
                    newBuffer.put(lineBuffer);
                    lineBuffer = newBuffer;
                }
                lineBuffer.put(lineBufferPos++, readBuffer.get());
            }
        }
    }

    /**
     * Fast RFC5424 syslog message parser.
     * Extracts only the MSG field, skipping all syslog headers.
     *
     * RFC5424 format:
     * <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
     *
     * Example:
     * <134>1 2024-01-15T10:30:00Z host app 1234 - - {"level":"INFO","msg":"test"}
     *
     * @param line Full syslog line
     * @return MSG field only (the actual log content)
     */
    private static String extractSyslogMessage(String line) {
        if (line == null || line.isEmpty() || line.charAt(0) != '<') {
            // Not valid syslog, return as-is
            return line;
        }

        try {
            int len = line.length();
            int pos = 1; // Skip '<'

            // Skip PRI: find '>'
            while (pos < len && line.charAt(pos) != '>') {
                pos++;
            }
            pos++; // Skip '>'

            // Skip VERSION and 6 space-delimited fields:
            // VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID
            for (int i = 0; i < 6 && pos < len; i++) {
                while (pos < len && line.charAt(pos) != ' ') {
                    pos++;
                }
                pos++; // Skip space
            }

            // Skip STRUCTURED-DATA: either "-" or "[...]"
            if (pos < len) {
                if (line.charAt(pos) == '-') {
                    pos++; // Skip '-'
                    if (pos < len && line.charAt(pos) == ' ') {
                        pos++; // Skip space after '-'
                    }
                } else if (line.charAt(pos) == '[') {
                    // Skip all structured data elements [...]
                    while (pos < len && line.charAt(pos) == '[') {
                        while (pos < len && line.charAt(pos) != ']') {
                            pos++;
                        }
                        pos++; // Skip ']'
                    }
                    if (pos < len && line.charAt(pos) == ' ') {
                        pos++; // Skip space after structured data
                    }
                }
            }

            // Everything remaining is the MSG
            if (pos < len) {
                return line.substring(pos);
            }

            return "";

        } catch (Exception e) {
            logger.warn("failed to parse syslog message, returning as-is: {}", e.getMessage());
            return line;
        }
    }

    @Override
    public int read() throws IOException {
        // This method is required by InputStream but readLine() is the main interface
        byte[] b = new byte[1];
        int result = read(b, 0, 1);
        return result == -1 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            return -1;
        }

        // Read a line and convert to bytes
        String line = readLine();
        if (line == null) {
            return -1;
        }

        byte[] lineBytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        int copyLen = Math.min(len, lineBytes.length);
        System.arraycopy(lineBytes, 0, b, off, copyLen);
        return copyLen;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        logger.info("closing Unix socket input stream: {}", socketPath);

        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            logger.warn("error closing client channel", e);
        }

        try {
            if (server != null) {
                server.close();
            }
        } catch (IOException e) {
            logger.warn("error closing server channel", e);
        }

        try {
            Files.deleteIfExists(Path.of(socketPath));
        } catch (IOException e) {
            logger.warn("error deleting socket file", e);
        }
    }

    @Override
    public String toString() {
        return "UnixSocketInputStream[path=" + socketPath + ", syslog=" + parseSyslog + "]";
    }
}
