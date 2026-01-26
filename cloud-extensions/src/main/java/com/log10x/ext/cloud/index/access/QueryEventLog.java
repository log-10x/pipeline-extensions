package com.log10x.ext.cloud.index.access;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.QueryLogLevel;

import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;

/**
 * CloudWatch Logs writer for query event logging.
 *
 * All query components (coordinator, scan workers, stream workers) within a
 * Quarkus pod share a single instance. Events are buffered per log stream and
 * flushed by a background thread every second, providing near-real-time
 * visibility without overwhelming CloudWatch API limits.
 *
 * The log group is specified per-call and must be pre-created (e.g. by
 * Terraform). Log streams are created on demand within that group.
 *
 * Log structure:
 *   Log Group:  (caller-specified, e.g. /log10x/query)
 *   Log Stream: {queryID}/{workerID}
 *
 * Lifecycle is managed by {@link AWSIndexAccess.AWSClients} — created lazily
 * on first access and shut down when the owning AWSClients is closed.
 */
public class QueryEventLog {

	private static final Logger logger = LogManager.getLogger(QueryEventLog.class);

	private static final long FLUSH_INTERVAL_MS = 1000;

	/**
	 * Maximum age in milliseconds before an idle stream buffer is evicted.
	 */
	private static final long IDLE_STREAM_EVICTION_MS = 5 * 60 * 1000;

	private final CloudWatchLogsAsyncClient cwClient;
	private final ScheduledExecutorService flusher;
	private final ScheduledFuture<?> flushTask;
	private final ConcurrentHashMap<String, StreamBuffer> buffers;
	private final ConcurrentHashMap.KeySetView<String, Boolean> createdStreams;
	private final ConcurrentLinkedQueue<CompletableFuture<?>> pendingPuts;

	private static class StreamBuffer {

		final String logGroup;
		final String streamName;
		final ConcurrentLinkedQueue<InputLogEvent> events = new ConcurrentLinkedQueue<>();
		volatile long lastEventTime;

		StreamBuffer(String logGroup, String streamName) {
			this.logGroup = logGroup;
			this.streamName = streamName;
			this.lastEventTime = System.currentTimeMillis();
		}
	}

	QueryEventLog() {
		this.cwClient = CloudWatchLogsAsyncClient.builder()
				.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
				.build();
		this.buffers = new ConcurrentHashMap<>();
		this.createdStreams = ConcurrentHashMap.newKeySet();
		this.pendingPuts = new ConcurrentLinkedQueue<>();

		this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "query-event-log-flusher");
			t.setDaemon(true);
			return t;
		});

		this.flushTask = this.flusher.scheduleAtFixedRate(
				this::flush, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	/**
	 * Buffers a structured log event for async delivery to CloudWatch Logs.
	 *
	 * @param logGroup the CloudWatch log group (must already exist)
	 * @param queryID  the query UUID (becomes part of the log stream name)
	 * @param workerID the pipeline UUID of the worker (becomes part of the log stream name)
	 * @param level    the log level
	 * @param message  the log message
	 * @param metadata optional key-value pairs for structured data (bytes, elapsed time, etc.)
	 */
	public void log(String logGroup, String queryID, String workerID,
		QueryLogLevel level, String message, Map<String, Object> metadata) {

		String streamName = queryID + "/" + workerID;
		String bufferKey = logGroup + "\0" + streamName;
		StreamBuffer buffer = buffers.computeIfAbsent(bufferKey,
				k -> new StreamBuffer(logGroup, streamName));

		StringBuilder json = new StringBuilder(128);
		json.append("{\"level\":\"").append(level.name())
			.append("\",\"message\":\"").append(escapeJson(message)).append('"');

		if (metadata != null && !metadata.isEmpty()) {
			json.append(",\"data\":{");
			boolean first = true;
			for (Map.Entry<String, Object> e : metadata.entrySet()) {
				if (!first) json.append(',');
				json.append('"').append(escapeJson(e.getKey())).append("\":");
				Object v = e.getValue();
				if (v instanceof Number) {
					json.append(v);
				} else {
					json.append('"').append(escapeJson(String.valueOf(v))).append('"');
				}
				first = false;
			}
			json.append('}');
		}
		json.append('}');

		buffer.events.add(InputLogEvent.builder()
				.timestamp(System.currentTimeMillis())
				.message(json.toString())
				.build());

		buffer.lastEventTime = System.currentTimeMillis();
	}

	/**
	 * Convenience overload — logs at INFO level with no metadata.
	 */
	public void log(String logGroup, String queryID, String workerID, String message) {
		log(logGroup, queryID, workerID, QueryLogLevel.INFO, message, null);
	}

	private static String escapeJson(String s) {
		if (s == null) return "";
		StringBuilder sb = null;
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c == '"' || c == '\\') {
				if (sb == null) {
					sb = new StringBuilder(s.length() + 8);
					sb.append(s, 0, i);
				}
				sb.append('\\').append(c);
			} else if (c == '\n') {
				if (sb == null) {
					sb = new StringBuilder(s.length() + 8);
					sb.append(s, 0, i);
				}
				sb.append("\\n");
			} else if (sb != null) {
				sb.append(c);
			}
		}
		return sb != null ? sb.toString() : s;
	}

	private void flush() {
		try {
			// Clean up completed futures
			pendingPuts.removeIf(CompletableFuture::isDone);

			long now = System.currentTimeMillis();

			Iterator<Map.Entry<String, StreamBuffer>> it = buffers.entrySet().iterator();

			while (it.hasNext()) {
				Map.Entry<String, StreamBuffer> entry = it.next();
				StreamBuffer buffer = entry.getValue();

				List<InputLogEvent> batch = drain(buffer.events);

				if (batch.isEmpty()) {
					if (now - buffer.lastEventTime > IDLE_STREAM_EVICTION_MS) {
						it.remove();
					}
					continue;
				}

				ensureLogStream(buffer.logGroup, buffer.streamName);

				CompletableFuture<?> future = cwClient.putLogEvents(PutLogEventsRequest.builder()
						.logGroupName(buffer.logGroup)
						.logStreamName(buffer.streamName)
						.logEvents(batch)
						.build())
						.whenComplete((response, err) -> {
							if (err != null) {
								logger.warn("failed to put query log events to {}/{}: {}",
										buffer.logGroup, buffer.streamName, err.getMessage());
							}
						});

				pendingPuts.add(future);
			}
		} catch (Exception e) {
			logger.warn("error during query event log flush", e);
		}
	}

	private static List<InputLogEvent> drain(ConcurrentLinkedQueue<InputLogEvent> queue) {
		List<InputLogEvent> batch = new ArrayList<>();
		InputLogEvent event;
		while ((event = queue.poll()) != null) {
			batch.add(event);
		}
		batch.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));
		return batch;
	}

	private void ensureLogStream(String logGroup, String streamName) {
		String compositeKey = logGroup + "\0" + streamName;
		if (!createdStreams.add(compositeKey)) {
			return;
		}

		try {
			cwClient.createLogStream(CreateLogStreamRequest.builder()
					.logGroupName(logGroup)
					.logStreamName(streamName)
					.build())
					.join();
		} catch (Exception e) {
			if (!isAlreadyExists(e)) {
				logger.warn("failed to create log stream {}/{}: {}", logGroup, streamName, e.getMessage());
				createdStreams.remove(compositeKey);
			}
		}
	}

	private static boolean isAlreadyExists(Throwable e) {
		if (e instanceof ResourceAlreadyExistsException) {
			return true;
		}
		if (e.getCause() instanceof ResourceAlreadyExistsException) {
			return true;
		}
		return false;
	}

	void shutdown() {
		flushTask.cancel(false);
		flusher.shutdown();

		try {
			flusher.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		// Final flush after scheduled executor is fully stopped
		flush();

		// Wait for all in-flight putLogEvents to complete before closing the client
		for (CompletableFuture<?> f : pendingPuts) {
			try {
				f.get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				// already logged by whenComplete
			}
		}

		cwClient.close();
		buffers.clear();
		createdStreams.clear();
	}
}
