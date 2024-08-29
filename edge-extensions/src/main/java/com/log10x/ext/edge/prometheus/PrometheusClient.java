package com.log10x.ext.edge.prometheus;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.log10x.l1x.prometheus.RemoteWrite.Label;
import com.log10x.l1x.prometheus.RemoteWrite.Sample;
import com.log10x.l1x.prometheus.RemoteWrite.TimeSeries;
import com.log10x.l1x.prometheus.RemoteWrite.WriteRequest;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class PrometheusClient implements Closeable {
	
	private static final Logger logger = LogManager.getLogger(PrometheusClient.class);

	public static final String PROMETHEUS_TIMESERIES_NAME = "__name__";

	protected class WriteRequestTask implements Runnable {
		
		private final WriteRequest request;
		
		protected WriteRequestTask(WriteRequest request) {
			this.request = request;
		}
		
		@Override
		public void run() {
			doSendWriteRequest(request);
		}
	}
	
	public final String host;
	
	private final String auth;

	private volatile ExecutorService executorService;

	public PrometheusClient(String host, String auth) {
		this.host = host;
		this.auth = auth;
	}

	private synchronized void validateExecutor() {
		
		if (this.executorService == null) {
			this.executorService = Executors.newSingleThreadExecutor();
		}
	}
	
	public boolean sendWriteRequest(WriteRequest writeRequest, boolean async) {
		
		if (async) {
			validateExecutor();
			
			this.executorService.submit(new WriteRequestTask(writeRequest));
			
			return true;
		} else {
			
			return doSendWriteRequest(writeRequest);
		}
	}

	private boolean doSendWriteRequest(WriteRequest writeRequest) {

		HttpURLConnection connection = null;

		try {

			URL url = new URI(this.host).toURL();
			
			connection = (HttpURLConnection) url.openConnection();

			if (auth != null) {
				connection.setRequestProperty("Authorization", auth);
			}

			connection.setRequestMethod("POST");

			// These are all based on the prometheus remote write specification
			// https://prometheus.io/docs/concepts/remote_write_spec/#protocol
			//
			connection.setRequestProperty("Content-Type", "application/x-protobuf");
			connection.setRequestProperty("Content-Encoding", "snappy");
			connection.setRequestProperty("X-Prometheus-Remote-Write-Version", "0.1.0");

			connection.setRequestProperty("User-Agent", "l10x-pipeline");

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

			if ((responseCode != HttpURLConnection.HTTP_OK) && (responseCode != HttpURLConnection.HTTP_NO_CONTENT)) {
				logger.warn("Failed remote write to {} - code {}.", host, responseCode);
				return false;
			}

			logger.debug("Successfully wrote to remote prometheus {}.", host);
			return true;

		} catch (Exception e) {
			
			throw new IllegalStateException("error connecting to:" + this.host, e);
			
		} finally {
			
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	@Override
	public synchronized void close() throws IOException {

		if (this.executorService != null) {

			this.executorService.shutdown();
			this.executorService = null;
		}
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append("PrometheusClient(").append(host).append(')').toString();
	}
	
	public static WriteRequest.Builder buildWriteRequest(CollectorRegistry registry) {
		return buildWriteRequest(registry, 0L);
	}

	public static WriteRequest.Builder buildWriteRequest(CollectorRegistry registry, long time) {

		long currentTime = (time > 0) ?
			time : 
			System.currentTimeMillis();

		Enumeration<Collector.MetricFamilySamples> metricFamilySamplesEnumeration = registry.metricFamilySamples();

		if (!metricFamilySamplesEnumeration.hasMoreElements()) {
			return null;
		}

		WriteRequest.Builder writeRequest = WriteRequest.newBuilder();

		while (metricFamilySamplesEnumeration.hasMoreElements()) {
			
			Collector.MetricFamilySamples metricFamilySamples = metricFamilySamplesEnumeration.nextElement();

			for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
				
				long timestamp = (sample.timestampMs != null ? 
					sample.timestampMs.longValue() : 
					currentTime);

				TimeSeries.Builder timeSeries = TimeSeries.newBuilder()
					.addSamples(Sample.newBuilder()
						.setValue(sample.value)
						.setTimestamp(timestamp))
					.addLabels(Label.newBuilder()
						.setName(PrometheusClient.PROMETHEUS_TIMESERIES_NAME)
						.setValue(sample.name)
					);

				int labelCount = (sample.labelNames != null) ? 
					sample.labelNames.size() : 
					0;

				for (int i = 0; i < labelCount; i++) {
					
					timeSeries.addLabels(
						Label.newBuilder().
							setName(sample.labelNames.get(i)).
							setValue(sample.labelValues.get(i)
						)
					);
				}

				writeRequest.addTimeseries(timeSeries);
			}
		}

		return writeRequest;
	}
}
