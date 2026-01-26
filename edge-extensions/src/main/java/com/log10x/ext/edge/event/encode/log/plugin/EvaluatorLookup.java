package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationAware;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin for evaluating an {@link https://doc.log10x.com/api/js/|10x JavaScript} expression against the target tenxObject.
 * 
 * For an example see: https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/forwarder/filebeat/input/log4j2.yaml
 */
@Plugin(
	name = EvaluatorLookup.PLUGIN_NAME, 
	category = StrLookup.CATEGORY)
public class EvaluatorLookup implements StrLookup, ConfigurationAware {

	public final static String PLUGIN_NAME = "eval";

	/**
	 * Static fallback lookup for early config parsing when factory is not yet set.
	 * This is set via setStaticFallbackLookup() before Log4j config loading.
	 */
	private static volatile StrLookup staticFallbackLookup;

	private StrLookup impl;
	private Configuration configuration;

	public EvaluatorLookup() {
		// Don't create impl here - factory might not be initialized yet
		// Use lazy initialization in getImpl()
	}

	/**
	 * Lazily initializes and returns the implementation.
	 * This is needed because Log4j2 instantiates plugins during config parsing,
	 * before the factory is set by the 10x engine.
	 */
	private StrLookup getImpl() {
		if (this.impl == null && factory != null) {
			this.impl = factory.createLookup();
			if (this.configuration != null && this.impl instanceof ConfigurationAware) {
				((ConfigurationAware)this.impl).setConfiguration(this.configuration);
			}
		}
		return this.impl;
	}

	@Override
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
		if (this.impl instanceof ConfigurationAware) {
			((ConfigurationAware)this.impl).setConfiguration(configuration);
		}
	}

	@Override
	public String lookup(String key) {
		StrLookup impl = getImpl();
		if (impl != null) {
			return impl.lookup(key);
		}
		// Fall back to static lookup for early config parsing
		if (staticFallbackLookup != null) {
			return staticFallbackLookup.lookup(key);
		}
		return null;
	}

	@Override
	public String lookup(LogEvent event, String key) {
		StrLookup impl = getImpl();
		if (impl != null) {
			return impl.lookup(event, key);
		}
		// Fall back to static lookup for early config parsing
		if (staticFallbackLookup != null) {
			return staticFallbackLookup.lookup(event, key);
		}
		return null;
	}

	/**
	 * Sets a static fallback lookup for early config parsing when factory is not yet set.
	 * This should be called before loading the Log4j configuration.
	 */
	public static void setStaticFallbackLookup(StrLookup lookup) {
		staticFallbackLookup = lookup;
	}
		
	
	/**
	 * this value is set by the 10x engine
	 */
	public static Factory factory;

	/**
	 * Sets the static fallback target used for evaluating expressions during
	 * early Log4j config parsing (before setConfiguration is called).
	 * This should be called before loading the Log4j configuration.
	 *
	 * @param target An EventFunctionTarget instance (passed as Object to avoid module dependency)
	 */
	public static void setStaticFallbackTarget(Object target) {
		if (factory != null) {
			factory.setStaticFallbackTarget(target);
		}
	}

	public static abstract class Factory {

		public abstract StrLookup createLookup();

		/**
		 * Sets the static fallback target for early config parsing.
		 * Default implementation does nothing - override in concrete factory.
		 *
		 * @param target An EventFunctionTarget instance (passed as Object to avoid module dependency)
		 */
		public void setStaticFallbackTarget(Object target) {
			// Default: no-op
		}
	}
}