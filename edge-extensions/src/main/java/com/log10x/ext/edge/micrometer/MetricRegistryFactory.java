package com.log10x.ext.edge.micrometer;

import java.io.IOException;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The MetricRegistryFactory defines the interface all classes used by
 * an l1x output to connect and write to a time-series destination should implement.

 * From an implementation standpoint the 'create' method is invoked by l1x via reflection
 * and as such a class registered as a metric registry factory does not 
 * need to strictly implement this interface as long as it provides a compatible
 * 'create' method. 
 */
public interface MetricRegistryFactory {

	/**
	 * This method is invoked by to create a MeterRegistry to which to write
	 * l1xSummary values.
	 * 
	 * l1x will use the result registry to create and increment tagged counters
	 * when writing to a target time-series destination.
	 * 
	 * @param 	options	 
	 * 			options provided for the registry via l1x launch arguments
	 *
	 * @return	the {@link io.micrometer.core.instrument.MeterRegistry} instance used to register and increment counter data
	 *
	 * @throws IOException if the registry could not be created
	 */
	public MeterRegistry create(Map<String, Object> options) throws IOException;
}
