package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 plugin for translating a time unit (e.g. 'Y') into 
 * a {@link java.time.format.DateTimeFormatter} compatible format
 */
@Plugin(name = DatePatternLookup.PLUGIN_NAME, 
		category = StrLookup.CATEGORY)
public class DatePatternLookup implements StrLookup {
	
	private static final String SEC_PATTERN = "%d{yyyy-MM-dd HH:mm:ss}";
	
	private static final String MIN_PATTERN = "%d{yyyy-MM-dd HH:mm}";

	private static final String HOUR_PATTERN = "%d{yyyy-MM-dd HH}";

	private static final String DAY_PATTERN = "%d{yyyy-MM-dd}";

	private static final String MONTH_PATTERN = "%d{yyyy-MM}";

	private static final String YEAR_PATTERN = "%d{yyyy}";

	private final static String ERROR = "";
	
	public final static String PLUGIN_NAME = "datePattern";

	private static final String intervalPattern(char unit) {
		
		switch (unit) {
		
		case 's': 
			return SEC_PATTERN;

		case 'm': 
			return MIN_PATTERN;

		case 'h': 
		case 'H': 
			return HOUR_PATTERN;
		
		case 'd': 
		case 'D': 
			return DAY_PATTERN;
		
		case 'M': 
			return MONTH_PATTERN;
			
		case 'y': 
		case 'Y': 
			return YEAR_PATTERN;

		}
		
		return null;
	}
	
	@Override
	public String lookup(String key) {
		
		if ((key == null) || (key.isEmpty())) {
			return ERROR;
		}
		
		String value = key.strip();
		
		if (value.length() < 2) {
			return ERROR;
		}
		
		char unit = value.charAt(value.length() - 1);
		
		String pattern = intervalPattern(unit);
		
		if (pattern == null) {
			return ERROR;
		}
		
		return pattern;
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return lookup(key);
	}
}
