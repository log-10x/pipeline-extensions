package com.log10x.ext.edge.event.encode.log.plugin;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j2 lookup plugin to translate a POSIX numeric permission set to its
 * alphanumeric equivalent. To learn more, see:
 * {@link https://en.wikipedia.org/wiki/File-system_permissions#Numeric_notation}
 * 
 * Used by: {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/forwarder/filebeat/input/log4j2.yaml}
 */
@Plugin(name = FilePermissionsLookup.PLUGIN_NAME, category = StrLookup.CATEGORY)
public class FilePermissionsLookup implements StrLookup {

	public final static String PLUGIN_NAME = "posixPermissions";

	public static int digitValue(char c) {
		return c - '0';
	}
	
	// based on: https://stackoverflow.com/questions/34234598/
	public static String formatUnixPermissions(String mask) {

		int len = mask.length();
		StringBuilder result = new StringBuilder(9);

		for (int i = 0; i < len; i++) {

			char c = mask.charAt(i);

			if (!Character.isDigit(c)) {

				throw new IllegalStateException("invalid octal char " + c + "at index " + i + " for: " + mask);
			}

			int num = digitValue(c);

			if ((num >= 8) || (num < 0)) {
				throw new IllegalStateException("invalid octal char " + c + "at index " + i + " for: " + mask);
			}

			result
				.append((num & 4) == 0 ? '-' : 'r')
				.append((num & 2) == 0 ? '-' : 'w')
				.append((num & 1) == 0 ? '-' : 'x');
		}

		return result.toString();
	}
		
	@Override
	public String lookup(String key) {

		if (key == null) {
			throw new IllegalArgumentException("could not parse null key");
		}

		if (key.isEmpty()) {
			throw new IllegalArgumentException("could not parse empty key");
		}

		if ((key.length() != 4) || (key.charAt(0) != '0')) {
			throw new IllegalArgumentException(key + " doesn't look like a legal octet posix permissions");
		}

		try {

			String octal = Integer.toString(Integer.parseInt(key.substring(1), 8), 8);
			
			return formatUnixPermissions(octal);

		} catch (Exception e) {
			throw new IllegalArgumentException("could not parse key: " + key);
		}
	}

	@Override
	public String lookup(LogEvent event, String key) {
		return lookup(key);
	}
}
