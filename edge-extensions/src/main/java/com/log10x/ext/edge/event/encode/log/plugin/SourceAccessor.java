package com.log10x.ext.edge.event.encode.log.plugin;

import java.util.function.Function;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

/*
 * A log4j2 plugin to provide programmatic access to target
 * tenxObject's tag via a {@link java.util.function.Function} reference.
 * 
 * To learn more about tenxObject source values, see:
 * {@link https://doc.log10x.com/run/input/stream/#inputsourcepattern}
 */
@Plugin(name = SourceAccessor.ELEMENT_TYPE, 
		category = Core.CATEGORY_NAME, 
		elementType = SourceAccessor.ELEMENT_TYPE, 
		printObject = true)

public abstract class SourceAccessor implements Function<Message, String> {

	public static final String ELEMENT_TYPE = "tag";

	/**
	 * A factory method to create a function instance that can be applied to
	 * an tenxObject implementing the logj2 Message interface to obtain it tag value.
	 * 
	 * @param 	tagField
	 * 			an optional 10x intrinsic / calculated / extracted field name to 
	 * 			return from the target object if it is NOT tagged.
	 * 
	 * @param 	tagValue
	 * 			default value to set as tag if the target object is NOT tagged and
	 * 			tagField is either not set or return a null value
	 * 
	 * 
	 * @return	a {@link java.util.function.Function} reference to which 
	 * 			an tenxObject implementing the {@link org.apache.logging.log4j.message.Message}
	 * 			interface can be passed via its apply method to return its tag
	 * 			value or the the field specified by {@code tagField}
	 * 
	 */
	@PluginFactory
    public static Function<Message, String> createAccessorMap(
    	@PluginAttribute(value="tagField") final String tagField,
    	@PluginAttribute(value="tagValue") final String tagValue) throws Exception {
   
    	return new SourceAccessorFunc(factory.createAccessor(tagField), tagValue);
    }
	
	public static Factory factory; 
	
	public static abstract class Factory {
		
	    public abstract Function<Message, String> createAccessor(String tagField) throws Exception;
	}
	
	private static class SourceAccessorFunc implements Function<Message, String> {

		private final Function<Message, String> sourceAccessor;
		private final String fallbackValue;
		
		SourceAccessorFunc(Function<Message, String> sourceAccessor, String fallbackValue) {
			this.sourceAccessor = sourceAccessor;
			this.fallbackValue = fallbackValue;
		}
		
		@Override
		public String apply(Message t) {
			String result = sourceAccessor.apply(t);
			
			if (result != null) {
				return result;
			}
			
			return fallbackValue;
		}
		
	}
}