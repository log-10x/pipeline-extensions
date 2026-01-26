package com.log10x.api.pipeline.launch;

/**
 * Provides constants for launching 10x engine instances and setting up
 * folder locations on disk via environment variables.
 * 
 * To learn more see {@link https://doc.log10x.com/run/bootstrap|bootstrap}
 */
public class PipelineLaunchOptions {

	// general constants

	public static final String TENX = "TENX";
	
	public static final String tenx = TENX.toLowerCase();

	public static final String DEFAULT_BACKEND_ENDPOINT = "https://prometheus.log10x.com/";

	public static final String USER_ENDPOINT	= "api/v1/user";
	public static final String RW_API_ENDPOINT	= "api/v1/write";
	
	public static final String TENX_API_KEY			= TENX + "_API_KEY";
	public static final String TENX_INCLUDE_PATHS	= TENX + "_INCLUDE_PATHS";
	
	public static final String NO_API_KEY			= "NO-API-KEY";
	
	// bootstrap launch arguments
	
	public static final String PIPELINE = "pipeline";
	public static final String UNIQUE_ID = "uniqueId";
	public static final String PARENT_ID = "parentPipelineId";
	public static final String RUN_TIME_NAME = "runtimeName";
	public static final String RUN_TIME_ATTR = "runtimeAttributes";
	public static final String API_KEY = "apiKey";
	public static final String BACKEND_ENDPOINT = "backendEndpoint";
	public static final String QUIET = "quiet";
	public static final String INCLUDE_PATHS = "includePaths";
	public static final String DISABLED_ARGS = "disabledArgs";
	public static final String ENV_DEBUG_VARS = "debugEnvVars";

	public static final String OVERRIDE = "override";

	public static final String OVERRIDE_KEY = "Key";
	public static final String OVERRIDE_REQUIRED = "Required";
	public static final String OVERRIDE_VALUE = "Value";

	public static final String DEFAULT_RUN_TIME_NAME = PipelineLaunchOptions.tenx;

	public static final String LOG_FILE_SYS_PROP = TENX + "_LOG_FILE";

	
	// configuration launch constants
	
	/**
	 * To learn more see {@link https://doc.log10x.com/run|run}
	 */
	public static final String RUN_PIPELINE  = "run";
	
	/**
	 * To learn more see {@link https://doc.log10x.com/compile|compile}
	 */
	public static final String COMPILE_PIPELINE  = "compile";

	/**
	 * To learn more see {@link https://doc.log10x.com/config/yaml/#include-directives|include}
	 */
	public static final String INCLUDE = "include";
	public static final String INCLUDE_SOURCE = "source";
	public static final String MACRO_PREFIX = "@";
}
