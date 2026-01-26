package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin to return the OS-appropriate log path.
 *
 * Checks an environment variable first, then falls back to OS-specific default paths.
 *
 * Usage in log4j2.yaml:
 * <pre>
 * value: "${tenxLogPath:envVar=TENX_LOG_PATH,windows=\\tenx\\log\\,linux=/var/log/tenx/}"
 * </pre>
 *
 * Parameters (passed in key string, comma-separated key=value pairs):
 * - envVar: Environment variable to check first (optional)
 * - windows: Path suffix appended to %AppData% on Windows
 * - linux: Full path on Linux/Unix systems
 */
@Plugin(name = TenxLogPathLookup.PLUGIN_NAME,
		category = StrLookup.CATEGORY)
public class TenxLogPathLookup implements StrLookup {

	public static final String PLUGIN_NAME = "tenxLogPath";

	private static final String PARAM_ENV_VAR = "envVar";
	private static final String PARAM_WINDOWS = "windows";
	private static final String PARAM_LINUX = "linux";
	private static final String PARAM_SEPARATOR = ",";
	private static final String KEY_VALUE_SEPARATOR = "=";

	private static final String OS_NAME_PROPERTY = "os.name";
	private static final String WINDOWS_PREFIX = "Windows";
	private static final String APPDATA_ENV = "AppData";

	@Override
	public String lookup(String key) {

		if (key == null || key.isEmpty()) {
			return null;
		}

		String envVar = null;
		String windowsPath = null;
		String linuxPath = null;

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
			} else if (PARAM_WINDOWS.equals(paramName)) {
				windowsPath = paramValue;
			} else if (PARAM_LINUX.equals(paramName)) {
				linuxPath = paramValue;
			}
		}

		if (envVar != null) {
			String override = System.getenv(envVar);
			if (override != null && !override.isEmpty()) {
				return override;
			}
		}

		String osName = System.getProperty(OS_NAME_PROPERTY);

		if (osName != null && osName.startsWith(WINDOWS_PREFIX)) {

			if (windowsPath != null) {
				String appData = System.getenv(APPDATA_ENV);
				if (appData != null) {
					return appData + windowsPath;
				}
			}

		} else {

			if (linuxPath != null) {
				return linuxPath;
			}
		}

		return null;
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return lookup(key);
	}
}
