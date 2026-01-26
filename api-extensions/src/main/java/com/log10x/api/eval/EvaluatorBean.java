package com.log10x.api.eval;

/**
 * This interface provides an entry point into the 10x engine expression evaluator.
 *
 * It allows for evaluating of 10x scripting expressions programmatically by
 * custom input and output streams.
 * 
 * An example of how this interface is used can be seen at {@link https://github.com/log-10x/pipeline-extensions/blob/main/cloud-extensions/src/main/java/com/log10x/ext/cloud/camel/EventCamelStreamRouteBuilder.java|EventCamelStreamRouteBuilder}
 * where the evaluator is made available to an Apache Camel context, allowing
 * for declarative message exchanges to access the 10x engine to dynamically
 * evaluate expressions.
 * 
 */
public interface EvaluatorBean {
		
	/**
	 * Queries the TenXInput the bean is currently associated with for a specified field value.
	 * To learn more see {@link https://doc.log10x.com/api/js/#TenXInput.get|TenXInput.get}
	 * 
	 * @param 	key	
	 * 			input field name.
	 * 
	 * @return	input field value or an empty string of not found
	 */
	public Object get(String key);

	/**
	 * Set a field for the TenXInput the bean is currently associated with.
	 * To learn more see {@link https://doc.log10x.com/api/js/#TenXInput.set|TenXInput.set}
	 * 
	 * @param 	key	
	 * 			input field name.
	 * 
	 * @param 	value	
	 * 			value to set
	 * 
	 * @return	field value prior to {@code value} being set or an empty value
	 * 			for a new insertion
	 */
	public Object set(String key, Object value);

	/**
	 * Evaluates a target 10x JavaScript expression
	 * 
	 * @param 	exp
	 * 			the expression to evaluate. This can be any valid .js scripting
	 * 			expression. To learn more see the {@link https://doc.log10x.com/api/js/|JavaScript API}.
	 * 
	 * @return	the evaluation result
	 */
	public Object eval(String exp);
	

	/**
	 * Evaluates a target 10x scripting expression for truthiness
	 * 
	 * @param 	exp
	 * 			the expression to evaluate as truthy. This can be any valid 10x .js scripting
	 * 			expression. To learn more see {@link https://doc.log10x.com/api/js/|JavaScript API}.
	 * 
	 * @return	whether the evaluation result is {@link https://developer.mozilla.org/en-US/docs/Glossary/Truthy|truthy}.
	 */
	public boolean truthy(String exp);

	/**
	 * Queries the 10x engine for the value an environment variable.
 	 * This function queries the list of environment variables which includes
     * any startup arguments passed into the process, as well as OS vars  and JVM system properties.
     * To learn more see the {@link https://doc.log10x.com/api/js/|JavaScript API}
	 * 
	 * @param 	var
	 * 			name of environment variable
	 * 
	 * @param 	defaultValue
	 * 			value to return if {@code var} has not been configured
	 * 
	 * @return	the resolved variable value or {@code defaultValue} if not found
	 */
	public Object env(String var, Object defaultValue);
	

	/**
	 * Queries the 10x engine for the value an environment variable.
 	 * This function queries the list of environment variables which includes
     * any startup arguments passed into the process, as well as OS vars  and JVM system properties.
     * To learn more see the {@link https://doc.log10x.com/api/js/|JavaScript API}
	 * 
	 * @param 	var
	 * 			name of environment variable
	 * 
	 * @param 	mustExist
	 * 			if true will throw an exception if not matching value found
	 * 
	 * @return	the resolved variable value 
	 */
	public default Object env(String var, boolean mustExist) {
		
		Object env = this.env(var, null);
		
		if ((env == null) && (mustExist)) {
			throw new IllegalStateException("launch argument or environment variable not found: " + var);
		}
		
		return env;
	}

	/**
	 * Queries the 10x engine for the value an environment variable.
 	 * This function queries the list of environment variables which includes
     * any startup arguments passed into the process, as well as OS vars  and JVM system properties.
     * To learn more see the {@link https://doc.log10x.com/api/js/|JavaScript API}
	 * 
	 * @param 	var
	 * 			name of environment variable
	 * 
	 * 
	 * @return	the resolved variable value 
	 */
	public default Object env(String var) {
		
		return this.env(var, false);
	}
	
	/**
	 * Sets a debug value for the current 10x input or output.
	 * This enables a custom 10x input or output to report on its state 
	 * via the setting of an atomic counter as {@code value} and incrementing its
	 * value at runtime to report to the 10x metrics backend.
	 * 
	 * To learn more about reporting see {@link https://doc.log10x.com/run/monitor/|10x monitoring}.
	 * 
	 * For an example of usage see {@link https://github.com/log-10x/pipeline-extensions/blob/main/cloud-extensions/src/main/java/com/log10x/ext/cloud/index/write/IndexFilterWriter.java|IndexFilterWriter}.
	 * 
	 * @param 	key
	 * 			name of debug value
	 * 
	 * @param 	value
	 * 			debug value to report
	 */
	public void debugState(String key, Object value);
}
