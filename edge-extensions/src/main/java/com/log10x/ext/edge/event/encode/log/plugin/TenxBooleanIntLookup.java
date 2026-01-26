package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin to return an integer value based on a boolean environment variable.
 *
 * Returns trueValue if the env var is truthy, falseValue if falsy or not set.
 *
 * Usage in log4j2.yaml:
 * <pre>
 * minSize: "${tenxBooleanInt:envVar=filebeatRotateOnStartup,trueValue=1,falseValue=2147483647}"
 * </pre>
 *
 * Parameters (passed in key string, comma-separated key=value pairs):
 * - envVar: Environment variable to check (required)
 * - trueValue: Value to return when env var is truthy (required)
 * - falseValue: Value to return when env var is falsy or not set (required)
 */
@Plugin(name = TenxBooleanIntLookup.PLUGIN_NAME,
		category = StrLookup.CATEGORY)
public class TenxBooleanIntLookup implements StrLookup {

	public static final String PLUGIN_NAME = "tenxBooleanInt";

	private static final String PARAM_ENV_VAR = "envVar";
	private static final String PARAM_TRUE_VALUE = "trueValue";
	private static final String PARAM_FALSE_VALUE = "falseValue";
	private static final String PARAM_SEPARATOR = ",";
	private static final String KEY_VALUE_SEPARATOR = "=";

	@Override
	public String lookup(String key) {

		if (key == null || key.isEmpty()) {
			return null;
		}

		String envVar = null;
		String trueValue = null;
		String falseValue = null;

		String[] params = key.split(PARAM_SEPARATOR);

		for (String param : params) {

			String trimmed = param.trim();
			int eqIndex = trimmed.indexOf(KEY_VALUE_SEPARATOR);

			if (eqIndex <= 0) {
				continue;
			}

			String paramName = trimmed.substring(0, eqIndex).trim();
			String paramValue = trimmed.substring(eqIndex + 1).trim();

			if (PARAM_ENV_VAR.equals(paramName)) {
				envVar = paramValue;
			} else if (PARAM_TRUE_VALUE.equals(paramName)) {
				trueValue = paramValue;
			} else if (PARAM_FALSE_VALUE.equals(paramName)) {
				falseValue = paramValue;
			}
		}

		if (envVar == null || trueValue == null || falseValue == null) {
			return falseValue;
		}

		String envValue = System.getenv(envVar);

		if (envValue == null || envValue.isEmpty()) {
			return falseValue;
		}

		return isTruthy(envValue) ? trueValue : falseValue;
	}

	/**
	 * Checks if a string value is considered "truthy".
	 * Truthy values: true, 1, yes, on, enabled (case insensitive)
	 */
	private boolean isTruthy(String value) {

		if (value == null || value.isEmpty()) {
			return false;
		}

		String lower = value.trim().toLowerCase();

		return "true".equals(lower) ||
			   "1".equals(lower) ||
			   "yes".equals(lower) ||
			   "on".equals(lower) ||
			   "enabled".equals(lower);
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return lookup(key);
	}
}
