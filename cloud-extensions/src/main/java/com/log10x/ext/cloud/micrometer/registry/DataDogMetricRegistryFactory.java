package com.log10x.ext.cloud.micrometer.registry;

import java.io.IOException;
import java.util.Map;

import com.log10x.ext.edge.micrometer.MapRegistryConfig;
import com.log10x.ext.edge.micrometer.MetricRegistryFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.datadog.DatadogConfig;
import io.micrometer.datadog.DatadogMeterRegistry;

/**
 * An implementation of the {@link MetricRegistryFactory} interface for Datadog
 */
public class DataDogMetricRegistryFactory implements MetricRegistryFactory {

	protected static class DatadogMapConfig extends MapRegistryConfig 
		implements DatadogConfig {

		public DatadogMapConfig(Map<String, Object> options) {
			super(options);
		}
	}
			
	@Override
	public DatadogMeterRegistry create(Map<String, Object> options) throws IOException {
				
		return new DatadogMeterRegistry(new DatadogMapConfig(options), Clock.SYSTEM);
	}
}