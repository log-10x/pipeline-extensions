package com.log10x.api.util.prometheus;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.log10x.prometheus.remotewrite.RemoteWrite.WriteRequest;

public class PrometheusClient implements Closeable {

	private static final Logger logger = LogManager.getLogger(PrometheusClient.class);

	public static final String PROMETHEUS_TIMESERIES_NAME = "__name__";
	public static final String PROMETHEUS_COUNTER_SUFFIX = "_total";

	public final String host;
	
	private final Map<String, String> auth;

	private volatile boolean closed;
	
	public PrometheusClient(String host, Map<String, String> auth) {
		this.host = host;
		this.auth = auth;
	}

	public boolean sendWriteRequest(WriteRequest writeRequest) {
		
		if (this.closed) {
			throw new IllegalStateException("closed");
		}
		
		HttpURLConnection connection = null;

		try {

			URL url = new URI(this.host).toURL();
			
			connection = (HttpURLConnection) url.openConnection();

			if (auth != null) {
				for (Map.Entry<String, String> entry : auth.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue();
					
					connection.setRequestProperty(key, value);
				}
			}

			connection.setRequestMethod("POST");

			// These are all based on the prometheus remote write specification
			// https://prometheus.io/docs/concepts/remote_write_spec/#protocol
			//
			connection.setRequestProperty("Content-Type", "application/x-protobuf");
			connection.setRequestProperty("Content-Encoding", "snappy");
			connection.setRequestProperty("X-Prometheus-Remote-Write-Version", "0.1.0");

			connection.setRequestProperty("User-Agent", "tenx-pipeline");

			byte[] encodedData = Snappy.compress(writeRequest.toByteArray());

			connection.setRequestProperty("Content-Length", String.valueOf(encodedData.length));
			connection.setDoOutput(true);

			// Write binary data to the request body
			try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
				
				outputStream.write(encodedData);
				outputStream.flush();
			}

			// Get the response code and handle it accordingly
			int responseCode = connection.getResponseCode();

			if ((responseCode != HttpURLConnection.HTTP_OK) &&
				(responseCode != HttpURLConnection.HTTP_NO_CONTENT)) {

				String metricNames = extractMetricNames(writeRequest);
				String responseBody = getResponseBody(connection);

				logger.warn("Failed to send metrics to {}. Response code: {}, Metrics: [{}], Response body: {}",
					this.host, responseCode, metricNames, responseBody);

				return false;
			}

			return true;

		} catch (Exception e) {

			String metricNames = extractMetricNames(writeRequest);

			logger.error("Error sending metrics to {}. Metrics: [{}]", this.host, metricNames, e);

			throw new IllegalStateException(
					"error connecting to:" + this.host + " while sending metrics: [" + metricNames + "]", e);
			
		} finally {
			
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	private static String extractMetricNames(WriteRequest writeRequest) {
		try {
			return writeRequest.getTimeseriesList().stream()
				.map(ts -> ts.getLabelsList().stream()
					.filter(label -> PROMETHEUS_TIMESERIES_NAME.equals(label.getName()))
					.map(label -> label.getValue())
					.findFirst()
					.orElse("unknown"))
				.distinct()
				.sorted()
				.collect(Collectors.joining(", "));
		} catch (Exception e) {
			logger.warn("Failed extracting metric names", e);
			return "Failed extracting metric names - " + e.getMessage();
		}
	}

	private static String getResponseBody(HttpURLConnection connection) {
		try (InputStream errorStream = connection.getErrorStream()) {
			if (errorStream == null) {
				return "No Error Stream";
			}

			return new String(errorStream.readAllBytes());
		} catch (Exception e) {
			logger.warn("Failed reading error stream", e);
			return "Failed reading error stream - " + e.getMessage();
		}
	}
	
	@Override
	public synchronized void close() throws IOException {

		if (this.closed) {
			return;
		}
		
		this.closed = true;
	}

	@Override
	public String toString() {
		
		return "PrometheusClient(" + this.host + ')';
	}
}
