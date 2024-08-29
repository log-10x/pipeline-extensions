package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationAware;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin use to evaluate an {@link http://doc.log10x.com/docs/api/js|l1x JavaScript} expression against the target l1xObject.
 * 
 * For an example, see: https://github.com/l1x-co/config/blob/main/pipelines/run/modules/input/forwarder/filebeat/input/log4j2.yaml
 */
@Plugin(
	name = EvaluatorLookup.PLUGIN_NAME, 
	category = StrLookup.CATEGORY)
public class EvaluatorLookup implements StrLookup, ConfigurationAware {
	
	public final static String PLUGIN_NAME = "eval";
	
	private StrLookup impl;
	
	public EvaluatorLookup() {
		this.impl = factory.createLookup();
	}
	
	@Override
	public void setConfiguration(Configuration configuration) {
		((ConfigurationAware)impl).setConfiguration(configuration);	
	}

	@Override
	public String lookup(String key) {
		return impl.lookup(key);
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return impl.lookup(event, key);
	}
		
	
	/**
	 * this value is set by the l1x runtime
	 */	
	public static Factory factory; 
	
	public static abstract class Factory {
		
		public abstract StrLookup createLookup();
	}
}