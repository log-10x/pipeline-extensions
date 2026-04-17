package com.log10x.ext.cloud.index.query.object;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.QueryLogLevel;
import com.log10x.ext.cloud.index.shared.BaseIndexWriter;

/**
 * Writer that buffers matched query events to a local temp file as JSONL and
 * uploads the file to object storage on close so that out-of-band consumers
 * (e.g. the Log10x MCP server) can retrieve full query results via S3 polling.
 *
 * Runs alongside {@link IndexObjectQueryWriter}, which continues to write the
 * byte-count markers the coordinator uses as a completion backstop. This writer
 * only adds result payloads — it does not replace or alter the backstop path.
 *
 * The writer enforces a per-worker event cap (configurable via
 * {@code queryObjectResultsMaxEvents}, default 2000). When the cap is reached,
 * subsequent events are dropped and a sibling {@code .truncated} marker object
 * is uploaded so the consumer can surface the truncation to the user.
 *
 * Results are written under a separate top-level prefix from the byte-count
 * markers so that {@link IndexQueryWriter#queryElapsed} — which parses every
 * last path segment under {indexObjectPath(query)}/{queryId}/ as a long — is
 * not disturbed by non-numeric result filenames:
 *
 *   {indexObjectPath(queryResults, target)}/{queryId}/{objectByteRangesKey}.jsonl
 *
 * Truncation marker (only written when the cap is hit):
 *   {indexObjectPath(queryResults, target)}/{queryId}/{objectByteRangesKey}.truncated
 */
public class IndexObjectQueryResultsWriter extends BaseIndexWriter {

	private static final int DEFAULT_MAX_EVENTS_PER_WORKER = 2000;

	private static final String RESULTS_SUFFIX = ".jsonl";

	private static final String TRUNCATED_SUFFIX = ".truncated";

	private final String queryId;

	private final String workerID;

	private final List<String> logLevels;

	private final String objectByteRangesKey;

	private final int maxEvents;

	private final Path tempFile;

	private final BufferedWriter tempWriter;

	private long eventCount;

	private long truncatedCount;

	private boolean truncated;

	private boolean closed;

	private boolean shouldLog(QueryLogLevel level) {
		if (logLevels == null || logLevels.isEmpty()) {
			return level != QueryLogLevel.DEBUG;
		}
		return logLevels.contains(level.name());
	}

	public IndexObjectQueryResultsWriter(Map<String, Object> args, EvaluatorBean evaluatorBean)
		throws NoSuchAlgorithmException, IllegalArgumentException, IOException {

		this(MapperUtil.jsonMapper.convertValue(args, IndexQueryObjectOptions.class), null, evaluatorBean);
	}

	protected IndexObjectQueryResultsWriter(IndexQueryObjectOptions options,
		ObjectStorageIndexAccessor indexAccessor,
		EvaluatorBean evaluatorBean) throws NoSuchAlgorithmException, IOException {

		super(options, indexAccessor, evaluatorBean);

		this.queryId = options.queryObjectID;
		this.workerID = (String) evaluatorBean.env(PipelineLaunchOptions.UNIQUE_ID);
		this.logLevels = options.queryObjectLogLevels;

		org.apache.logging.log4j.ThreadContext.put("queryId", this.queryId);

		Object capEnv = evaluatorBean.env("queryObjectResultsMaxEvents");
		int parsedCap = DEFAULT_MAX_EVENTS_PER_WORKER;
		if (capEnv != null) {
			try {
				parsedCap = Integer.parseInt(capEnv.toString());
			} catch (NumberFormatException ignored) {
			}
		}
		this.maxEvents = parsedCap > 0 ? parsedCap : DEFAULT_MAX_EVENTS_PER_WORKER;

		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(options.queryObjectTarget.getBytes());
		md.update(options.queryObjectTargetObject.getBytes());

		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		for (long rangeValue : options.queryObjectByteRanges) {
			buffer.putLong(0, rangeValue);
			md.update(buffer);
		}

		this.objectByteRangesKey = Base64.getUrlEncoder().withoutPadding()
			.encodeToString(md.digest());

		this.tempFile = Files.createTempFile("tenx-query-results-", RESULTS_SUFFIX);
		this.tempWriter = new BufferedWriter(new OutputStreamWriter(
			Files.newOutputStream(this.tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING),
			StandardCharsets.UTF_8));
	}

	@Override
	public synchronized void flush() throws IOException {

		try {
			if (closed) {
				super.flush();
				return;
			}

			if (currChars.builder.length() == 0) {
				super.flush();
				return;
			}

			if (eventCount >= maxEvents) {
				truncated = true;
				truncatedCount++;
				super.flush();
				return;
			}

			// Guard: scan for characters that would break the JSONL line
			// contract or produce invalid JSON. The upstream serializer
			// should escape these inside string values but doesn't always
			// (Ticket 0 — 6% corruption rate from unescaped \n in the
			// text field). This boundary check catches any that slip through.
			CharSequence content = currChars.builder;
			if (containsControlChar(content)) {
				tempWriter.append(escapeControlChars(content));
			} else {
				tempWriter.append(content);
			}
			tempWriter.newLine();
			eventCount++;

		} catch (IOException e) {

			if (shouldLog(QueryLogLevel.ERROR)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
					QueryLogLevel.ERROR,
					String.format("results writer error during flush: %s", e.getMessage()),
					null);
			}
			throw e;

		} finally {
			super.flush();
		}
	}

	@Override
	public synchronized void close() throws IOException {

		if (closed) {
			return;
		}
		closed = true;

		IOException uploadError = null;

		try {
			tempWriter.flush();
			tempWriter.close();

			String basePath = indexAccessor.indexObjectPath(IndexObjectType.queryResults, this.options.target());
			String resultsPrefix = (new StringBuilder(basePath))
				.append(indexAccessor.keyPathSeperator())
				.append(((IndexQueryObjectOptions) this.options).queryObjectID)
				.toString();

			long fileSize = Files.size(tempFile);

			if (eventCount > 0) {

				try (InputStream in = new FileInputStream(tempFile.toFile())) {
					indexAccessor.putObject(resultsPrefix,
						this.objectByteRangesKey + RESULTS_SUFFIX,
						in, fileSize, null);
				}
			}

			if (truncated) {

				byte[] emptyBytes = new byte[0];
				try (InputStream empty = new java.io.ByteArrayInputStream(emptyBytes)) {
					indexAccessor.putObject(resultsPrefix,
						this.objectByteRangesKey + TRUNCATED_SUFFIX,
						empty, 0L, null);
				}
			}

			if (shouldLog(QueryLogLevel.PERF)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
					QueryLogLevel.PERF,
					String.format("results writer complete: %d events written, %d dropped, %d bytes",
						eventCount, truncatedCount, fileSize),
					Map.of("resultEvents", eventCount,
						"resultsTruncated", truncatedCount,
						"resultsBytes", fileSize));
			}

		} catch (IOException e) {

			uploadError = e;

			if (shouldLog(QueryLogLevel.ERROR)) {
				indexAccessor.logQueryEvent(this.queryId, this.workerID,
					QueryLogLevel.ERROR,
					String.format("results writer error during close: %s", e.getMessage()),
					null);
			}

		} finally {

			try {
				Files.deleteIfExists(tempFile);
			} catch (IOException ignored) {
			}

			org.apache.logging.log4j.ThreadContext.remove("queryId");
			super.close();
		}

		if (uploadError != null) {
			throw uploadError;
		}
	}

	/**
	 * Fast scan for control characters (0x00–0x1F) that would break JSON
	 * or the JSONL line contract. Returns false for the common case (clean
	 * content), avoiding any allocation.
	 */
	private static boolean containsControlChar(CharSequence cs) {
		for (int i = 0, len = cs.length(); i < len; i++) {
			if (cs.charAt(i) < 0x20) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Escape all JSON-illegal control characters in the serialized line.
	 * Only called when {@link #containsControlChar} returns true.
	 *
	 * Maps the common whitespace controls to their JSON escape sequences
	 * (\n, \r, \t) and everything else to the Unicode escape form.
	 */
	private static String escapeControlChars(CharSequence cs) {
		StringBuilder sb = new StringBuilder(cs.length() + 32);
		for (int i = 0, len = cs.length(); i < len; i++) {
			char c = cs.charAt(i);
			if (c >= 0x20) {
				sb.append(c);
			} else {
				switch (c) {
					case '\n': sb.append('\\').append('n'); break;
					case '\r': sb.append('\\').append('r'); break;
					case '\t': sb.append('\\').append('t'); break;
					default:
						sb.append('\\').append('u')
							.append('0').append('0')
							.append(Character.forDigit((c >> 4) & 0xF, 16))
							.append(Character.forDigit(c & 0xF, 16));
						break;
				}
			}
		}
		return sb.toString();
	}
}
