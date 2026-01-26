package com.log10x.ext.cloud.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

/**
 * Utility base class for Apache Camel route processors
 */
public abstract class BaseRouteProcessor implements Processor {

	public static final String STREAM = "stream";

	protected static String routeStrProp(Exchange exchange, String key) {

		return routeStrProp(exchange, key, true);
	}

	protected static String routeStrProp(Exchange exchange, String key, boolean tryLowercase) {

		Object value = routeProp(exchange, key, tryLowercase, true);
		return value.toString();
	}

	protected static int routeIntProp(Exchange exchange, String key, int defaultValue) {
		Object value = routeProp(exchange, key, true, false);

		if (value == null) {
			return defaultValue;
		}

		try {
			return Integer.parseInt(value.toString());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	protected static Object routeProp(Exchange exchange, String key) {
		return routeProp(exchange, key, true, true);
	}

	protected static Object routeProp(Exchange exchange, String key, boolean tryLowercase, boolean mustExist) {

		Object value = exchange.getProperty(key);

		if (value == null) {

			value = exchange.getMessage().getHeader(key);

			if (value == null) {

				if (tryLowercase) {
					value = routeProp(exchange, key.toLowerCase(), false, mustExist);
				}

				if ((value == null) &&
					(mustExist)) {

					throw new IllegalStateException(
						"header: " + key + " not found in message: " + exchange.getProperties());
				}
			}
		}

		return value;
	}
}
