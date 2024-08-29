package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;

/**
 * A log4j2 filter plugin that evaluates an {@link http://doc.log10x.com/docs/api/js/#L1xObject|l1x JavaScript} expression against the target l1xObject to log
 */
@Plugin(name = ExpressionFilter.PLUGIN_NAME, 
		category = Core.CATEGORY_NAME, 
		elementType = Filter.ELEMENT_TYPE, 
		printObject = true)

public abstract class ExpressionFilter extends AbstractFilter {
	
	public static final String PLUGIN_NAME = "l1xFilter";
		
    @PluginFactory
    public static AbstractFilter createFilter(
    	@PluginAttribute("expression") final String expression, 
    	@PluginAttribute("onMatch") final Result match,
        @PluginAttribute("onMismatch") final Result mismatch) throws Exception {
       
    	return factory.createFilter(expression, match, mismatch);
    }
    
    /**
   	 * this value is set by the l1x runtime
   	 */
	public static Factory factory; 
	
	public static abstract class Factory {
		
	    public abstract AbstractFilter createFilter(String expression,
	    	Result match, Result mismatch) throws Exception;
	}
}