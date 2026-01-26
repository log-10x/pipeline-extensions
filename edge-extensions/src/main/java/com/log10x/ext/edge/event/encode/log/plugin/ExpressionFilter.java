package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;

/**
 * A log4j2 filter plugin that evaluates an {@link https://doc.log10x.com/api/js/#TenXObject|10x JavaScript} expression against the target tenxObject to log
 */
@Plugin(name = ExpressionFilter.PLUGIN_NAME, 
		category = Core.CATEGORY_NAME, 
		elementType = Filter.ELEMENT_TYPE, 
		printObject = true)

public abstract class ExpressionFilter extends AbstractFilter {
	
	public static final String PLUGIN_NAME = "tenxFilter";
		
    @PluginFactory
    public static AbstractFilter createFilter(
    	@PluginAttribute("expression") final String expression, 
    	@PluginAttribute("onMatch") final Result match,
        @PluginAttribute("onMismatch") final Result mismatch) throws Exception {
       
    	return factory.createFilter(expression, match, mismatch);
    }
    
    /**
   	 * this value is set by the 10x engine
   	 */
	public static Factory factory; 
	
	public static abstract class Factory {
		
	    public abstract AbstractFilter createFilter(String expression,
	    	Result match, Result mismatch) throws Exception;
	}
}