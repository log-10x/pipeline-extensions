package com.log10x.ext.edge.event.encode.log.plugin;

import java.util.function.Function;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

/*
 * A log4j2 plugin to provide programmatic access to target
 * l1x object's tag via a {@link java.util.function.Function} reference.
 * 
 * To learn more about l1xObject source values, see:
 * {@link http://doc.log10x.com/run/input/stream/#inputsourcepattern}
 */
@Plugin(name = SourceAccessor.ELEMENT_TYPE, 
		category = Core.CATEGORY_NAME, 
		elementType = SourceAccessor.ELEMENT_TYPE, 
		printObject = true)

public abstract class SourceAccessor implements Function<Message, String> {

	public static final String ELEMENT_TYPE = "tag";

	/**
	 * A factory method to create a function instance that can be applied to
	 * an l1x Object implementing the logj2 Message interface to obtain it tag value.
	 * 
	 * @param 	tagField
	 * 			an optional l1x intrinsic / calculated / extracted field name to 
	 * 			return from the target object if it is NOT tagged.
	 * 
	 * 
	 * @return	a {@link java.util.function.Function} reference to which 
	 * 			an l1x Object implementing the {@link org.apache.logging.log4j.message.Message}
	 * 			interface can be passed via its apply method to return its tag
	 * 			value or the the field specified by {@code tagField}
	 * 
	 */
	@PluginFactory
    public static Function<Message, String> createAccessorMap(
    	@PluginAttribute(value="tagField") final String tagField) throws Exception {
   
    	return factory.createAccessor(tagField);
    }
	
	public static Factory factory; 
	
	public static abstract class Factory {
		
	    public abstract Function<Message, String> createAccessor(String tagField) throws Exception;
	}
}