package com.log10x.ext.edge.micrometer.registry;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.LoggerContext;

import com.log10x.api.util.micrometer.MapRegistryConfig;
import com.log10x.api.util.micrometer.MetricRegistryFactory;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.EmittingMetricRegistry;
import com.log10x.api.util.micrometer.PeriodicEmittingMetricRegistry.PeriodicEmittingConfig;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class LogMetricRegistryFactory implements MetricRegistryFactory {

	public static final String LOGGER_NAME = "loggerName";

	protected static class LogMapConfig extends MapRegistryConfig implements SimpleConfig, PeriodicEmittingConfig {

		public LogMapConfig(Map<String, Object> options) {
			super(options);
		}
	}

	protected static class LogMetricRegistry extends SimpleMeterRegistry implements EmittingMetricRegistry {

		private final Logger logger;

		protected LogMetricRegistry(LogMapConfig config, Logger logger) {

			super(config, Clock.SYSTEM);

			this.logger = logger;
		}

		@Override
		public String toString() {

			String result = this.getMetersAsString();
			return result;
		}

		@Override
		public void emit() {

			if ((logger.isInfoEnabled()) && (!getMeters().isEmpty())) {

				try {

					logger.info(this);

				} catch (Exception e) {

					logger.error("error logging values", e);
				}
			}
		}
	}

	@Override
	public MeterRegistry create(Map<String, Object> options) throws IOException {

		LogMapConfig config = new LogMapConfig(options);

		Duration step = config.emitStep();
		String loggerName = config.get(LOGGER_NAME, true);

		LoggerContext ctxt = LogManager.getContext(this.getClass().getClassLoader(), false);
		
		Logger logger = ctxt.getLogger(loggerName);

		EmittingMetricRegistry internalRegistry = new LogMetricRegistry(config, logger);

		return new PeriodicEmittingMetricRegistry(internalRegistry, step.toMillis());
	}
}
