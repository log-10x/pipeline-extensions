package com.log10x.ext.edge.event.encode.log.plugin;

import java.io.File;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin to return the OS directory file separator.
 * Used by: {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/forwarder/filebeat/input/log4j2.yaml}
 */
@Plugin(name = FileSeperatorLookup.PLUGIN_NAME, 
		category = StrLookup.CATEGORY)
public class FileSeperatorLookup implements StrLookup {
	
	public final static String PLUGIN_NAME = "fileSeperator";

	@Override
	public String lookup(String key) {		
		return File.separator;		
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return lookup(key);
	}
}
