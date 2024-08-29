package com.log10x.ext.edge.event.encode.log.plugin;

import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

/*
 * A log4j2 plugin to provide programmatic access to target
 * l1xObject fields via a {@link java.util.function.Function} reference.
 */
@Plugin(name = FieldValuesAccessor.ELEMENT_TYPE, 
		category = Core.CATEGORY_NAME, 
		elementType = FieldValuesAccessor.ELEMENT_TYPE, 
		printObject = true)

public abstract class FieldValuesAccessor implements 
	Function<Message, Map<String, Object>> {

	public static final String ELEMENT_TYPE = "l1xFields";
	
	public static final String FIELD_NAMES = "fieldNames";
	
	/**
	 * A factory method to create a function instance that can be applied to
	 * an l1x Object implementing the logj2 Message interface to obtain a set
	 * of target field values.  
	 * 
	 * @param 	fieldNames
	 * 			a list of l1x intrinsic / calculated / extracted object values.
	 * 			Field names are comma separated (e.g., ("text,vars,price")
	 * 
	 * 
	 * @return	a {@link java.util.function.Function} reference into which to pass 
	 * 			an l1xObject implementing the {@link org.apache.logging.log4j.message.Message}
	 * 			interface via its apply method to receive a map of the desired field
	 * 			values pairs specified by {@code fieldNames}
	 * 
	 */
	@PluginFactory
    public static Function<Message, Map<String, Object>> createAccessorMap(
    	@PluginAttribute(value=FIELD_NAMES) final String fieldNames) {
       	
		return factory.createAccessor(fieldNames);
    }	
	
	/**
	 * This member is set by the l1x runtime.
	 */
	public static Factory factory; 
	
	public static abstract class Factory {
		
		public abstract Function<Message, Map<String, Object>> createAccessor(
			String fieldNames);
	}
}