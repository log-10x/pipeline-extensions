package com.log10x.api.util.micrometer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.micrometer.core.instrument.config.MeterRegistryConfig;

/*
 * This class provides a convenient way of mapping 10x launch arguments
 * passed to a registry to its micrometer.io configuration. By deriving
 * from it child classes can utilize the provided 'get' method implementation
 * which is geared towards obtaining configuration values from the Map
 * supplied to it by a MetricRegistryFactory's 'create' method. 
 */
public abstract class MapRegistryConfig implements MeterRegistryConfig {

	private static final Logger logger = LogManager.getLogger(MapRegistryConfig.class);

	private static final char OPTION_SEPERATOR = '.';
	
	protected final Map<String, Object> options;
	
	private static String camelCase(String s) {
		
		if (s.isEmpty()) {
			return s;
		}
		
		char c = s.charAt(0);
		
		String s0 = String.valueOf(Character.toLowerCase(c));
		
		return (s.length() > 1) ?
			s0 + s.substring(1) : 
			s0;
	}
	
	public MapRegistryConfig(Map<String, Object> options) {
		
		String prefix = this.prefix();
		
		if (prefix != null) {
		
			this.options = new HashMap<>(options.size());

			for (Entry<String, Object> entry : options.entrySet()) {
				
				String key = entry.getKey();
				Object value = entry.getValue();
				
				String mapKey = (key.startsWith(prefix)) ?
					key.substring(prefix.length()) : 
					key;
				
				String camelcaseKey = camelCase(mapKey);
				
				this.options.put(camelcaseKey, value);
				this.options.put(this.prefix() + OPTION_SEPERATOR + camelcaseKey, value);
			}
			
		} else {
			
			this.options = new HashMap<>(options);
		}	
		
		if (logger.isDebugEnabled()) {	
			
			logger.debug("metric registry: "  + this.prefix() +
				" options: " + this.options);
		}
	}
	
	public String get(String key, boolean mustExist) {
		
		String result = this.get(key);
		
		if ((result == null) && (mustExist)) {
			
			throw new IllegalStateException("no '" + key +
				"' option provided in: " + this.options);
		}
		
		return result;
	}

	
	@Override
	public String get(String key) {
			
		Object value = options.get(key);
		
		String result = (value != null) ?
			value.toString(): 
			null;
		
		if (logger.isTraceEnabled()) {
			
			logger.trace("metric registry: " + this.prefix() +
				" requested key: " + key + ", found:" + result);
		}
		
		return result;
	}
	
	@Override
	public String toString() {
		return options.toString();
	}
}
