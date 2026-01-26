package com.log10x.ext.edge.micrometer.registry;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.util.HttpUtil;
import com.log10x.api.util.micrometer.MapRegistryConfig;
import com.log10x.api.util.micrometer.MetricRegistryFactory;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.EmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.PeriodicEmittingConfig;
import com.log10x.api.util.prometheus.PrometheusClient;
import com.log10x.api.util.prometheus.PrometheusRequestBuilder;
import com.log10x.prometheus.remotewrite.RemoteWrite.WriteRequest;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

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
		private final PrometheusRequestBuilder requestBuilder;

		protected PrometheusRWMetricRegistry(PrometheusRWMapConfig config, PrometheusClient client) {

			super(config);

			this.client = client;
			this.requestBuilder = new PrometheusRequestBuilder();
		}

		@Override
		public void emit() {

			WriteRequest.Builder writeRequest = this.requestBuilder.buildWriteRequest(
					this.getPrometheusRegistry());

			if (writeRequest != null) {
				try {
					client.sendWriteRequest(writeRequest.build());
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

		Map<String, String> auth = auth(config);

		PrometheusClient client = new PrometheusClient(host, auth);

		EmittingMetricRegistry internalRegistry = new PrometheusRWMetricRegistry(config, client);

		return new PeriodicEmittingMetricRegistry(internalRegistry, step.toMillis());
	}

	private static Map<String, String> auth(PrometheusRWMapConfig config) {

		String user = config.get(USER);

		if (user != null) {

			String pass = config.get(PASS, true);

			String basic = HttpUtil.basicAuth(user, pass);
			
			return Map.of("Authorization", basic);
		}

		String token = config.get(TOKEN);

		if (token != null) {
			String bearer = HttpUtil.bearerAuth(token);
			
			return Map.of("Authorization", bearer);
		}

		return null;
	}
}
