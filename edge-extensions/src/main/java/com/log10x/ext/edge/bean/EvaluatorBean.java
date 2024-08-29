package com.log10x.ext.edge.bean;

/**
 * The EvaluatorBean interface provides an entry point into the l1x run-time.
 * It allows for evaluating of l1x scripting expressions programmatically by
 * custom input and output streams.
 * 
 * An example of how this interface is used can be seen at {@link com.log10x.l1x.camel.EventCamelStreamRouteBuilder}
 * where the evaluator is made available to an Apache Camel context, allowing
 * for declarative message exchanges to access the l1x run-time to dynamically
 * evaluate expressions.
 * 
 */
public interface EvaluatorBean {
		
	/**
	 * Queries the current l1x object's dictionary for a value.
	 * To learn more, see the {@link L1xDict} in {@link /config/pipelines/run/l1x.js}
	 * 
	 * @param 	key	
	 * 			dictionary key name.
	 * 
	 * @return	dictionary value or an empty string of not found
	 */
	public Object dict(String key);

	/**
	 * Sets the current l1x object's dictionary for a value.
	 * To learn more, see the {@link L1xDict} in {@link /config/pipelines/run/l1x.js}
	 * 
	 * @param 	key	
	 * 			dictionary key name.
	 * 
	 * @param 	value	
	 * 			value to set
	 * 
	 * @return	dictionary value prior to {@code value} being set or an empty 
	 * 			for a new insertion
	 */
	public Object dict(String key, Object value);

	/**
	 * Evaluates a target l1x scripting expression
	 * 
	 * @param 	exp
	 * 			the expression to evaluate. This can be any valid l1x .js scripting
	 * 			expression. To learn more about l1x scripting, see: 
	 * 			{@link /config/pipelines/run/l1x.js}
	 * 
	 * @return	the evaluation result
	 */
	public Object eval(String exp);
	

	/**
	 * Evaluates a target l1x scripting expression for truthiness
	 * 
	 * @param 	exp
	 * 			the expression to evaluate as truthy. This can be any valid l1x .js scripting
	 * 			expression. To learn more about l1x scripting, see: 
	 * 			{@link /config/pipelines/run/l1x.js}
	 * 
	 * @return	whether the evaluation result is truthy {@link https://developer.mozilla.org/en-US/docs/Glossary/Truthy}
	 */
	public boolean truthy(String exp);

	/**
	 * Queries the l1x run-time for the value an environment variable.
 	 * This function queries the list of environment variables which includes
     * any startup arguments passed into the process, as well as OS vars  and JVM system properties.
     * To learn more, see the {@link L1xEnv} in {@link /config/pipelines/run/l1x.js}
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
	 * Queries the l1x run-time for the value an environment variable.
 	 * This function queries the list of environment variables which includes
     * any startup arguments passed into the process, as well as OS vars  and JVM system properties.
     * To learn more, see the {@link L1xEnv} in {@link /config/pipelines/run/l1x.js}
	 * 
	 * @param 	var
	 * 			name of environment variable
	 * 
	 * @param 	defaultValue
	 * 			value to return if {@code var} has not been configured
	 * 
	 * @return	the resolved variable value or empty string if not found
	 */
	public Object env(String var);	
	
	/**
	 * Sets a debug value for the current l1x input or output.
	 * This enables a custom l1x input or output to report on its state 
	 * via the setting of an atomic counter as {@code value} and incrementing its
	 * value as run-time to report to the l1x metrics backend.
	 * 
	 * To learn more about debug reporting, see: {@link /config/pipelines/run/config/debug.yaml}
	 * 
	 * For an example of code usage, see: {@link com.log10x.l1x.index.write.IndexFilterWriter#IndexFilterWriter} 
	 * 
	 * @param 	key
	 * 			name of debug value
	 * 
	 * @param 	value
	 * 			debug value to report
	 */
	public void debugState(String key, Object value);
}
