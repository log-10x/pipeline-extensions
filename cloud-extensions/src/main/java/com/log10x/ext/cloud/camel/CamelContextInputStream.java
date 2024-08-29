package com.log10x.ext.cloud.camel;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.log10x.ext.cloud.camel.util.stream.OpenPipedOutputStream;
import com.log10x.ext.edge.bean.EvaluatorBean;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * Loads an Apache Camel route and read its output into an l1x pipeline. 
 */
public class CamelContextInputStream extends PipedInputStream {
	
	private static final Logger logger = LogManager.getLogger(CamelContextInputStream.class);

	public static class Options {
		
		@JsonMerge
		protected final String name;

		/**
		 * YAML definition of route
		 */
		@JsonMerge
		protected final String route;

		Options() {
			this(null, null);
		}

		Options(String name, String route) {
			this.name = name;
			this.route = route;
		}
	}
	
	protected final Options options;

	protected final CamelContext camelContext;
	private final OpenPipedOutputStream openPipedOutputStream;
	
	public CamelContextInputStream(Map<String, Object> args, 
		EvaluatorBean bean) throws Exception {
		
		this(MapperUtil.noFailUnknownJsonMapper.convertValue(args, Options.class), bean);
	}
	
	public CamelContextInputStream(Options options,
		EvaluatorBean bean) throws Exception {
			
		if (bean == null) {
			throw new IllegalArgumentException("bean");
		}
		
		if (options == null) {
			throw new IllegalArgumentException("options");
		}
		
		if (options.name == null) {
			throw new IllegalArgumentException("options.name");
		}
		
		if (options.route == null) {
			throw new IllegalArgumentException("options.route");
		}
		
		this.options = options;
	
		this.openPipedOutputStream = new OpenPipedOutputStream(this);
				
		RouteBuilder builder = new EventCamelStreamRouteBuilder(
			options.name , options.route, this.openPipedOutputStream, bean);
		
		this.camelContext = new DefaultCamelContext();
		
		if (logger.isDebugEnabled()) {
			logger.debug("starting route context: " + options.name);
		}
		
		if (logger.isTraceEnabled()) {
			logger.debug("route: " + options.route);
		}
		
		this.camelContext.start();	
		this.camelContext.addRoutes(builder);
		
		if (logger.isDebugEnabled()) {
			logger.debug("started route context: " + options.name);
		}
	}

	@Override
	public String toString() {
		return options.name;
	}

	@Override
	public void close() throws IOException {

		try {
			this.openPipedOutputStream.terminate();
		} catch (Exception e) {
			logger.error("error closing output stream:" + this, e);
		}

		try {

			this.camelContext.shutdown();
		} catch (Exception e) {

			logger.error("error closing context:" + this, e);
		}

		super.close();
	}
}
