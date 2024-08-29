package com.log10x.ext.edge.micrometer.registry;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import com.log10x.ext.edge.micrometer.MapRegistryConfig;
import com.log10x.ext.edge.micrometer.MetricRegistryFactory;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

public class PrometheusMetricRegistryFactory implements MetricRegistryFactory, Closeable {

	protected static class PrometheusMapConfig extends MapRegistryConfig 
		implements PrometheusConfig {

		public PrometheusMapConfig(Map<String, Object> options) {
			super(options);
		}

		public Integer port() {
			return PropertyValidator.getInteger(this, "port").orElse(DEFAULT_PORT);
		}
	}

	public static final int DEFAULT_PORT = 9100;

	protected HTTPServer httpServer;

	@Override
	public PrometheusMeterRegistry create(Map<String, Object> options) throws IOException {

		PrometheusMapConfig config = new PrometheusMapConfig(options);

		int port = config.port();

		PrometheusMeterRegistry result = new PrometheusMeterRegistry(config);

		this.httpServer = new HTTPServer(new InetSocketAddress(port), 
			result.getPrometheusRegistry(), true);

		return result;
	}

	@Override
	public void close() throws IOException {
		httpServer.close();
	}
}
