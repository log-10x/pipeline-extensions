package com.log10x.ext.edge.micrometer.registry;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.util.micrometer.MapRegistryConfig;
import com.log10x.api.util.micrometer.MetricRegistryFactory;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.EmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.PeriodicEmittingConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.pushgateway.PushGateway;

public class PrometheusPGMetricRegistryFactory implements MetricRegistryFactory {

	private static final String HOST = "host";
	private static final String JOB = "job";

	protected static class PrometheusPGMapConfig extends MapRegistryConfig
			implements PrometheusConfig, PeriodicEmittingConfig {

		public PrometheusPGMapConfig(Map<String, Object> options) {
			super(options);
		}

		@Override
		public String prefix() {
			return "prometheusPG";
		}
	}

	private static final Logger logger = LogManager.getLogger(PrometheusPGMetricRegistry.class);

	protected static class PrometheusPGMetricRegistry extends PrometheusMeterRegistry
			implements EmittingMetricRegistry {

		private final String host;
		private final PushGateway pushGateway;

		protected PrometheusPGMetricRegistry(PrometheusPGMapConfig config, String host, String job) {

			super(config);

			this.host = host;
			this.pushGateway = PushGateway.builder()
					.address(host)
					.job(job)
					.registry(this.getPrometheusRegistry())
					.build();
		}

		@Override
		public void emit() {
			try {
				pushGateway.pushAdd();
			} catch (IOException e) {
				logger.warn("Failed pushing to push gateway {}.", host, e);
			}
		}
	}

	@Override
	public MeterRegistry create(Map<String, Object> options) throws IOException {
		PrometheusPGMapConfig config = new PrometheusPGMapConfig(options);

		Duration step = config.emitStep();
		String host = config.get(HOST, true);
		String job = config.get(JOB, true);

		EmittingMetricRegistry internalRegistry = new PrometheusPGMetricRegistry(config, host, job);

		return new PeriodicEmittingMetricRegistry(internalRegistry, step.toMillis());
	}
}
