package com.log10x.ext.edge.event.encode.log.plugin;

import java.nio.charset.Charset;

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;

/**
 * log4j2 layout plugin for logging a set of target fields values
 * from tenxObjects written to this output.
 * For an example use, see: {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/output/event/appender/unix/log4j2.yaml}
 */
@Plugin(name = EncodeFieldsLayout.PLUGIN_NAME, 
		category = Node.CATEGORY, 
		elementType = Layout.ELEMENT_TYPE, 
		printObject = true)
public abstract class EncodeFieldsLayout extends AbstractStringLayout {
    
	public static final String PLUGIN_NAME = "tenxLayout";
		
    @PluginFactory
    public static AbstractStringLayout createLayout(
        @PluginAttribute(value = "charset", defaultString = "UTF-8") final Charset charset,
    	
        /**
         * list of tenxObject fields
         */
        @PluginAttribute(value = "encodedFields") String encodedFields,
    	
        /**
         * 10x JavaScript expression that must evaluate as truthy against
         * the current tenxObject to write its {@code encodedFields}
         */
        @PluginAttribute(value = "filter") String filter,
    	
        /**
         * the format to use when writing {@code encodedFields}. Supported:
         * 	- json: encode field and value names as a JSON object
         *  - delimited: encode field values delimited by {@code encodeDelimiter}
         */
        @PluginAttribute(value = "encodeType") String encodeType,
    	
        /**
         * delimiter to use when {@code encodeType} is set to: 'delimited'
         */
        @PluginAttribute(value = "encodeDelimiter", defaultChar=' ') char encodeDelimiter,
    	
        /**
         * an optional 10x JavaScript expression whose value is written to output
         * when the 10x pipeline has reached eof.
         */
        @PluginAttribute(value = "eofSignalExp") String eofSignalExp,

        /**
         * an optional 10x JavaScript expression whose value is written to output
         * when the 10x pipeline is flushed.
         */
        @PluginAttribute(value = "flushSignalExp") String flushSignalExp,

        /**
         * an optional 10x JavaScript expression whose value is written to output
         * when the 10x pipeline is queued.
         */
        @PluginAttribute(value = "queueSignalExp") String queueSignalExp    		
    	) throws Exception {
        
    	return factory.createLayout(charset, encodedFields,
    		filter, encodeType, encodeDelimiter, 
    		eofSignalExp, flushSignalExp, queueSignalExp);
    }
    
    /**
	 * this value is set by the 10x engine
	 */
	public static Factory factory; 
	
	public static abstract class Factory {
		
		public abstract AbstractStringLayout createLayout(
		Charset charset, String encodedFields,
		String filter, String encodeType, char encodeDelimiter,
		String eofSignalExp, String flushSignalExp, String queueSignalExp) throws Exception;
	}
	
	private EncodeFieldsLayout() {
		super(null);
	}
}