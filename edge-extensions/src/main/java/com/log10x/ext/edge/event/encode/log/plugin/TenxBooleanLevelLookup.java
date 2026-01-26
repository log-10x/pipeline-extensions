package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin to return a log level based on a boolean environment variable.
 *
 * Returns "ALL" if the env var is truthy, "OFF" if falsy or not set.
 *
 * Usage in log4j2.yaml:
 * <pre>
 * level: "${tenxBooleanLevel:envVar=filebeatLoggingToStderr,defaultEnabled=true}"
 * level: "${tenxBooleanLevel:envVar=filebeatLoggingToSyslog}"
 * </pre>
 *
 * Parameters (passed in key string, comma-separated key=value pairs):
 * - envVar: Environment variable to check (required)
 * - defaultEnabled: Default value when env var is not set (optional, defaults to false)
 */
@Plugin(name = TenxBooleanLevelLookup.PLUGIN_NAME,
		category = StrLookup.CATEGORY)
public class TenxBooleanLevelLookup implements StrLookup {

	public static final String PLUGIN_NAME = "tenxBooleanLevel";

	private static final String PARAM_ENV_VAR = "envVar";
	private static final String PARAM_DEFAULT_ENABLED = "defaultEnabled";
	private static final String PARAM_SEPARATOR = ",";
	private static final String KEY_VALUE_SEPARATOR = "=";

	private static final String LEVEL_ALL = "ALL";
	private static final String LEVEL_OFF = "OFF";

	@Override
	public String lookup(String key) {

		if (key == null || key.isEmpty()) {
			return LEVEL_OFF;
		}

		String envVar = null;
		boolean defaultEnabled = false;

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
			} else if (PARAM_DEFAULT_ENABLED.equals(paramName)) {
				defaultEnabled = isTruthy(paramValue);
			}
		}

		if (envVar == null) {
			return defaultEnabled ? LEVEL_ALL : LEVEL_OFF;
		}

		String envValue = System.getenv(envVar);

		if (envValue == null || envValue.isEmpty()) {
			return defaultEnabled ? LEVEL_ALL : LEVEL_OFF;
		}

		return isTruthy(envValue) ? LEVEL_ALL : LEVEL_OFF;
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
