package com.log10x.ext.edge.micrometer.registry;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class PrometheusRWMetricRegistryFactory implements MetricRegistryFactory {

	private static final String HOST = "host";
	private static final String USER = "user";
	private static final String TOKEN = "token";
	private static final String PASS = "password";

	protected static class PrometheusRWMapConfig extends MapRegistryConfig
			implements PrometheusConfig, PeriodicEmittingConfig {

		public PrometheusRWMapConfig(Map<String, Object> options) {
			super(options);
		}

		@Override
		public String prefix() {
			return "prometheusRW";
		}
	}

	private static final Logger logger = LogManager.getLogger(PrometheusRWMetricRegistry.class);

	protected static class PrometheusRWMetricRegistry extends PrometheusMeterRegistry
			implements EmittingMetricRegistry {

		private final PrometheusClient client;

		protected PrometheusRWMetricRegistry(PrometheusRWMapConfig config, PrometheusClient client) {

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

	@Override
	public MeterRegistry create(Map<String, Object> options) throws IOException {

		PrometheusRWMapConfig config = new PrometheusRWMapConfig(options);

		Duration step = config.emitStep();

		String host = config.get(HOST, true);

		String encodedAuth = auth(config);

		PrometheusClient client = new PrometheusClient(host, encodedAuth);

		EmittingMetricRegistry internalRegistry = new PrometheusRWMetricRegistry(config, client);

		return new PeriodicEmittingMetricRegistry(internalRegistry, step.toMillis());
	}

	private static String auth(PrometheusRWMapConfig config) {

		String user = config.get(USER);

		if (user != null) {

			String pass = config.get(PASS, true);

			return HttpUtil.basicAuth(user, pass);
		}

		String token = config.get(TOKEN);

		if (token != null) {
			return HttpUtil.bearerAuth(token);
		}

		return null;
	}
}
