package com.log10x.ext.edge.micrometer.registry;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.micrometer.MapRegistryConfig;
import com.log10x.ext.edge.micrometer.MetricRegistryFactory;
import com.log10x.ext.edge.micrometer.registry.PeriodicEmittingMetricRegistry.EmittingMetricRegistry;
import com.log10x.ext.edge.micrometer.registry.PeriodicEmittingMetricRegistry.PeriodicEmittingConfig;
import com.log10x.ext.edge.prometheus.PrometheusClient;
import com.log10x.ext.edge.util.HttpUtil;
import com.log10x.l1x.prometheus.RemoteWrite.WriteRequest;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class PrometheusL1xMetricRegistryFactory implements MetricRegistryFactory {

	protected static class PrometheusL1xMapConfig extends MapRegistryConfig
			implements PrometheusConfig, PeriodicEmittingConfig {

		public PrometheusL1xMapConfig(Map<String, Object> options) {
			super(options);
		}

		@Override
		public String prefix() {
			return "prometheusL1x";
		}
	}

	private static final Logger logger = LogManager.getLogger(PrometheusL1xMetricRegistry.class);

	protected static class PrometheusL1xMetricRegistry extends PrometheusMeterRegistry
			implements EmittingMetricRegistry {

		private final PrometheusClient client;

		protected PrometheusL1xMetricRegistry(PrometheusL1xMapConfig config, PrometheusClient client) {

			super(config);

			this.client = client;
		}

		@Override
		public void emit() {

			WriteRequest.Builder writeRequest = PrometheusClient.buildWriteRequest(this.getPrometheusRegistry());

			if (writeRequest != null) {
				try {
					client.sendWriteRequest(writeRequest.build(), false);
				} catch (Exception e) {
					logger.warn("Failed sending write request to {}.", client.host, e);
				}
			}
		}
	}

	private final EvaluatorBean bean;

	public PrometheusL1xMetricRegistryFactory(EvaluatorBean bean) {
		this.bean = bean;
	}

	@Override
	public MeterRegistry create(Map<String, Object> options) throws IOException {

		PrometheusL1xMapConfig config = new PrometheusL1xMapConfig(options);

		Duration step = config.emitStep();

		String host = (String) this.bean.env("metricsEndpoint");
		String token = (String) this.bean.env("licenseKey");

		String encodedAuth = HttpUtil.bearerAuth(token);

		PrometheusClient client = new PrometheusClient(host, encodedAuth);

		EmittingMetricRegistry internalRegistry = new PrometheusL1xMetricRegistry(config, client);

		return new PeriodicEmittingMetricRegistry(internalRegistry, step.toMillis());
	}
}
