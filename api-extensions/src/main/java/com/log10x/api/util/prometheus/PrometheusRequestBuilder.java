package com.log10x.api.util.prometheus;

import java.util.HashSet;
import java.util.Set;

import com.log10x.api.util.HashUtil;
import com.log10x.prometheus.remotewrite.RemoteWrite;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot.InfoDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Label;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.StateSetSnapshot.StateSetDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.UnknownSnapshot.UnknownDataPointSnapshot;

public class PrometheusRequestBuilder {

	private static final int ZERO_COUNTER_TIMESTAMP_OFFSET = 1000 * 60; // one minute
	
	private final Set<Long> reportedCounters;
	
	public PrometheusRequestBuilder() {

		this.reportedCounters = new HashSet<>();
	}

	public RemoteWrite.WriteRequest.Builder buildWriteRequest(PrometheusRegistry registry) {
		return buildWriteRequest(registry, 0L);
	}

	public synchronized RemoteWrite.WriteRequest.Builder buildWriteRequest(PrometheusRegistry registry, long time) {

		long currentTime = (time > 0) ?
			time : 
			System.currentTimeMillis();

		MetricSnapshots snapshots = registry.scrape();

		if (snapshots.size() == 0) {
			return null;
		}

		RemoteWrite.WriteRequest.Builder writeRequest = RemoteWrite.WriteRequest.newBuilder();

		for (MetricSnapshot snapshot : snapshots) {

			for (DataPointSnapshot dataPoint : snapshot.getDataPoints()) {

				boolean isCounter = (snapshot instanceof CounterSnapshot);
				
				String metricName = snapshot.getMetadata().getName();
				
				if (isCounter) {
					metricName += PrometheusClient.PROMETHEUS_COUNTER_SUFFIX;
				}
				
				RemoteWrite.TimeSeries.Builder timeSeries = RemoteWrite.TimeSeries.newBuilder()
					.addLabels(RemoteWrite.Label.newBuilder()
						.setName(PrometheusClient.PROMETHEUS_TIMESERIES_NAME)
						.setValue(metricName)
					);

				for (Label label : dataPoint.getLabels()) {

					timeSeries
							.addLabels(
							RemoteWrite.Label.newBuilder().
								setName(label.getName()).
								setValue(label.getValue())
						);
				}

				long timestamp = (dataPoint.hasScrapeTimestamp() ? 
						dataPoint.getScrapeTimestampMillis() : 
						currentTime);
				
				double dataPointValue = value(dataPoint);
				
				// Prometheus counters starting from non-zero value misbehave
				// when being asked to calculate stuff like increase()
				//
				if (isCounter) {
					
					long hash = hashSample(metricName, dataPoint);
					
					if ((this.reportedCounters.add(hash)) &&
						(dataPointValue > 0)) {
						
						timeSeries.addSamples(RemoteWrite.Sample.newBuilder()
								.setValue(0)
								.setTimestamp(timestamp - ZERO_COUNTER_TIMESTAMP_OFFSET));
					}
				}
				
				timeSeries.addSamples(RemoteWrite.Sample.newBuilder()
						.setValue(dataPointValue)
						.setTimestamp(timestamp));
				
				writeRequest.addTimeseries(timeSeries);
			}
		}

		return writeRequest;
	}

	private static double value(DataPointSnapshot dataPoint) {
		if (dataPoint == null) {
			throw new NullPointerException();
		}

		if (dataPoint instanceof CounterDataPointSnapshot counter) {
			return counter.getValue();
		} else if (dataPoint instanceof GaugeDataPointSnapshot gauge) {
			return gauge.getValue();
		} else if (dataPoint instanceof SummaryDataPointSnapshot summary) {
			return summary.getSum(); // or handle count/quantiles as needed
		} else if (dataPoint instanceof InfoDataPointSnapshot) {
			return 1.0; // Info metrics typically have a value of 1
		} else if (dataPoint instanceof StateSetDataPointSnapshot) {
			return 1.0; // Adjust based on specific state, if needed
		} else if (dataPoint instanceof UnknownDataPointSnapshot unknown) {
			return unknown.getValue();
		} else {
			throw new IllegalArgumentException("Unsupported data point - " + dataPoint.getClass().getName());
		}
	}

	private static long hashSample(String metricName, DataPointSnapshot dataPoint) {

		long result = HashUtil.hash(metricName);

		for (Label label : dataPoint.getLabels()) {

			result = HashUtil.hash(result, label.getName());
			result = HashUtil.hash(result, label.getValue());
		}

		return result;
	}
}
