package com.log10x.ext.cloud.micrometer.registry;

import java.io.IOException;
import java.util.Map;

import com.log10x.api.util.micrometer.MapRegistryConfig;
import com.log10x.api.util.micrometer.MetricRegistryFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.signalfx.SignalFxConfig;
import io.micrometer.signalfx.SignalFxMeterRegistry;

/**
 * An implementation of the {@link MetricRegistryFactory} interface for signalFX
 */
public class SignalFxMetricRegistryFactory implements MetricRegistryFactory {

	protected static class SignalFxMapConfig extends MapRegistryConfig 
		implements SignalFxConfig {

		public SignalFxMapConfig(Map<String, Object> options) {
			super(options);
		}
	}
	
	@Override
	public SignalFxMeterRegistry create(Map<String, Object> options) throws IOException {

		return new SignalFxMeterRegistry(new SignalFxMapConfig(options), 
			Clock.SYSTEM);
	}
}
