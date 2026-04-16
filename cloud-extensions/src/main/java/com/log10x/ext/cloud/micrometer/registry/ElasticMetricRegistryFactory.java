package com.log10x.ext.cloud.micrometer.registry;

import java.io.IOException;
import java.util.Map;

import com.log10x.api.util.micrometer.MapRegistryConfig;
import com.log10x.api.util.micrometer.MetricRegistryFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.elastic.ElasticConfig;
import io.micrometer.elastic.ElasticMeterRegistry;

/**
 * An implementation of the {@link MetricRegistryFactory} interface for ElasticSearch
 * 
 * @see <a href="https://doc.log10x.com/run/output/metric/elastic/">ElasticSearch Output</a>
 */
public class ElasticMetricRegistryFactory implements MetricRegistryFactory {

	protected static class ElaticMapConfig extends MapRegistryConfig 
		implements ElasticConfig {

		public ElaticMapConfig(Map<String, Object> options) {
			super(options);
		}
	}
	
	@Override
	public ElasticMeterRegistry create(Map<String, Object> options) throws IOException {

		return new ElasticMeterRegistry(new ElaticMapConfig(options), 
			Clock.SYSTEM);
	}	
}