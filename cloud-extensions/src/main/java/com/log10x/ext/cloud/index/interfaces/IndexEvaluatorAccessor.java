package com.log10x.ext.cloud.index.interfaces;

import com.log10x.ext.edge.bean.EvaluatorBean;

/**
 * Helper interface for obtaining an {@link com.log10x.l1x.bean.EvaluatorBean} instance
 */
public interface IndexEvaluatorAccessor {

	public EvaluatorBean evaluator();
	
	/**
	 * Invokes an l1x function with a supplied parameter. Commonly used
	 * to translate string duration (e.g. "1min") and byte size values (e.g. "5KB")
	 * to their numeric equivalents.
	 * 
	 * @param 	func	
	 * 			name of l1x scripting function (e.g. "parseDuration")
	 * 
	 * @param 	value
	 * 			value to format (e.g. "1min")
	 * 
	 * @return	formatted value
	 */
	public default long parse(String func, String value) {
		
		return (value != null) ?
				((Number)evaluator().eval(String.format("%s(\"%s\")", 
					func, value))).longValue() : 
				0;
	}

}
