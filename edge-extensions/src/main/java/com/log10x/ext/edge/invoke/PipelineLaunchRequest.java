package com.log10x.ext.edge.invoke;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * A utility class used to generate the JSON request body for
 * launching a remote l1x pipeline accessible via a REST endpoint. 
 */
public class PipelineLaunchRequest {

	public static final String L1X			= "L1X_DEFAULT_PIPELINE_TYPE";
	public static final String LAUNCH_ARGS	= "L1X_DEFAULT_LAUNCH_ARGS";
	public static final String CLI_ARGS		= "L1X_DEFAULT_CLI_ARGS";
	public static final String OPTIONS		= "L1X_DEFAULT_OPTIONS";
	
	public static final String RUN_PIPELINE  = "run";
	public static final String MACRO_PREFIX = "@";
	
	public static final String INCLUDE = "include";
	public static final String INCLUDE_SOURCE = "source";
	
	// TODO - should be imported from PipelineLauncherOptions
	public static final String UNIQUE_ID_ARG	= "uniqueId";
	public static final String RUNTIME_NAME_ARG	= "runtimeName";
	
	/**
	 * Type of l1x pipeline to launch, i.e 'run', 'compile', etc.
	 */
	public final String l1x;
	
	/**
	 * An id for tracking this l1x pipeline request.
	 * Otherwise, the value is UUID.randomUUID().toString()
	 */
	public final String uuid;
	
	/**
	 * Launch arguments to give the launched pipeline.
	 * Used to pass launch specific arguments, like 'configFolder', 'licenseKey', etc.
	 */
	public final List<String> l1xLaunchArgs;
	
	/**
	 * Cli arguments to give the launched pipeline.
	 * Useful for passing launch configs via "@my/config/path" or simple global key/value pairs. 
	 */
	public final List<String> l1xCliArgs;
	
	/**
	 * Complex options passing into the pipeline.
	 * Useful for constructing full pipeline config from a given yaml/json.
	 */
	public final Map<String, Object> l1xOptions;
	
	private PipelineLaunchRequest() {

		this(RUN_PIPELINE, uuid(), Collections.emptyList(),
				Collections.emptyList(), Collections.emptyMap());
	}
	
	private PipelineLaunchRequest(String l1x, String uuid,
			List<String> launchArgs, List<String> cliArgs, Map<String, Object> options) {

		this.l1x = l1x;
		this.uuid = uuid;
		
		this.l1xLaunchArgs = launchArgs;
		this.l1xCliArgs = cliArgs;
		this.l1xOptions = options;
	}
	
	public PipelineLaunchRequest merge(Object... values) {
		return doMerge(false, values);
	}
	
	public PipelineLaunchRequest override(Object... values) {
		return doMerge(true, values);
	}
	
	private PipelineLaunchRequest doMerge(boolean override, Object... values) {
	
		Builder builder = this.toBuilder();
		
		for (Object value : values) {
			builder.with(value, override);
		}
		
		if (override) {
			builder.withUuid(uuid());
		}
		
		return builder.build();
	}
	
	public String[] asPipelineArgs() throws JsonProcessingException {
		List<String> allArgs = new LinkedList<>();
		
		allArgs.add(UNIQUE_ID_ARG);
		allArgs.add(this.uuid);
		
		allArgs.addAll(this.l1xLaunchArgs);
		allArgs.add(this.l1x);
		allArgs.addAll(this.l1xCliArgs);
		
		if (!this.l1xOptions.isEmpty()) {
			String optionsAsJson = MapperUtil.jsonMapper.writeValueAsString(this.l1xOptions);
			
			allArgs.add(MACRO_PREFIX + optionsAsJson);
		}
		
		return allArgs.toArray(new String[0]);
	}
	
	/*
	 * Renders a JSON object
	 */
	@Override
	public String toString() {
		return safeToJson();
	}

	public String safeToJson() {
		try {
			return toJson();
		} catch (JsonProcessingException e) {
			return e.toString();
		}
	}

	public String toJson() throws JsonProcessingException {
		return MapperUtil.jsonMapper.writeValueAsString(this);
	}
	
	public Builder toBuilder() {
		return new Builder(this.l1x, this.uuid, this.l1xLaunchArgs, this.l1xCliArgs, this.l1xOptions);
	}
	
	public static PipelineLaunchRequest from(Map<String, Object> map) {
		return MapperUtil.noFailUnknownJsonMapper.convertValue(map, PipelineLaunchRequest.class);
	}
	
	/**
	 * @return A new PipelineLaunchRequest from default values in environment variables
	 */
	public static PipelineLaunchRequest fromEnv() {
		
		String l1x = envVal(L1X, PipelineLaunchRequest.RUN_PIPELINE);
		List<String> launchArgs = envList(LAUNCH_ARGS);
		List<String> cliArgs = envList(CLI_ARGS);
		Map<String, Object> options = envMap(OPTIONS);
		
		return newBuilder()
				.withL1x(l1x)
				.withLaunchArgs(launchArgs)
				.withCliArgs(cliArgs)
				.withOptions(options, true)
				.build();
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {
		
		private String l1x;
		private String uuid;
		private final List<String> l1xLaunchArgs;
		private final List<String> l1xCliArgs;
		private final Map<String, Object> l1xOptions;
		
		Builder() {
			this.l1x = RUN_PIPELINE;
			this.uuid = uuid();
			this.l1xLaunchArgs = new ArrayList<>();
			this.l1xCliArgs = new ArrayList<>();
			this.l1xOptions = new LinkedHashMap<>();
		}
		
		Builder(String l1x, String uuid,
				List<String> launchArgs, List<String> cliArgs, Map<String, Object> options) {
			
			this.l1x = l1x;
			this.uuid = uuid;
			this.l1xLaunchArgs = new ArrayList<>(launchArgs);
			this.l1xCliArgs = new ArrayList<>(cliArgs);
			this.l1xOptions = new LinkedHashMap<>(options);
		}
		
		public Builder withL1x(String l1x) {
			
			this.l1x = l1x;
			
			return this;
		}
		
		public Builder withUuid(String uuid) {
			
			this.uuid = uuid;
			
			return this;
		}
		
		public Builder withIncludes(String... includes) {
			
			List<Map<String, Object>> headerIncludes = new ArrayList<>(includes.length);
			
			for (String include : includes) {
				headerIncludes.add(Collections.singletonMap(INCLUDE_SOURCE, include));
			}
			
			Map<String, Object> includesMap = Collections.singletonMap(INCLUDE, headerIncludes);
			
			mergeMaps(this.l1xOptions, includesMap, false);
			
			return this;
		}

		public Builder withLaunchArg(String k, String v) {

			this.l1xLaunchArgs.add(k);
			this.l1xLaunchArgs.add(v);

			return this;
		}

		public Builder withLaunchArgs(List<String> launchArgs) {

			this.l1xLaunchArgs.addAll(launchArgs);

			return this;
		}

		public Builder withCliArg(String k, String v) {

			this.l1xCliArgs.add(k);
			this.l1xCliArgs.add(v);

			return this;
		}

		public Builder withCliArgs(List<String> cliArgs) {

			this.l1xCliArgs.addAll(cliArgs);

			return this;
		}
		
		public Builder withOptions(Map<String, Object> options, boolean override) {
			
			mergeMaps(this.l1xOptions, options, override);
			
			return this;
		}
		
		public Builder with(Object value, boolean override) {
			
			if (value == null) {
   				return this;
   			}
   			
   			if (value instanceof PipelineLaunchRequest) {
   				
   				PipelineLaunchRequest other = (PipelineLaunchRequest)value;
   				
   				if (!this.l1x.equals(other.l1x)) {
   					throw new IllegalArgumentException("Can't merge launch requests from two types:\n" + this + "\n" + other);
   				}
   				
   				withLaunchArgs(other.l1xLaunchArgs);
   				withCliArgs(other.l1xCliArgs);
   				withOptions(other.l1xOptions, override);
   				
   			} else if (value instanceof Map<?, ?>) {
   				
   				@SuppressWarnings("unchecked")
				Map<String, Object> other = (Map<String, Object>)value;
   				
   				withOptions(other, override);
   				
   			} else if (value instanceof List<?>) {
   				
   				@SuppressWarnings("unchecked")
				List<String> other = (List<String>)value;
   				
   				withCliArgs(other);
   				
   			} else {
   				
   				Map<String, Object> other = MapperUtil.jsonMapper.convertValue(value, MapperUtil.MAP_REF);
   				
   				withOptions(other, override);
   			}
			
			return this;
		}
		
		public PipelineLaunchRequest build() {
			
			return new PipelineLaunchRequest(l1x, uuid, l1xLaunchArgs, l1xCliArgs, l1xOptions);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void mergeMaps(Map<String, Object> to, Map<String, Object> from, boolean override) {
		
		for (Map.Entry<String, Object> entry : from.entrySet()) {
			
			String key = entry.getKey();
			Object value = entry.getValue();
			
			Object currentValue = to.get(key);
			
			if (currentValue == null) {
				to.put(key, value);
				continue;
			}
			
			if ((value instanceof List) &&
				(currentValue instanceof List)) {
				
				Collection<Object> currentCollection = new ArrayList<>((List<Object>)currentValue);
				currentCollection.addAll((Collection<Object>) value);
				
				to.put(key, currentCollection);
				
				continue;
			}
			
			if ((value instanceof Map) &&
				(currentValue instanceof Map)) {
				
				Map<String, Object> currentMap = new LinkedHashMap<>((Map<String, Object>)currentValue);
				Map<String, Object> map = (Map<String, Object>)value;
				
				mergeMaps(currentMap, map, override);
				
				to.put(key, currentMap);
				
				continue;
			}
			
			if (value.getClass() == currentValue.getClass()) {

				if (override) {
					to.put(key, value);
				}
				
				continue;
			}
			
			StringBuilder builder = new StringBuilder()
				.append("Incompatible types for overriding key - ")
				.append(key)
				.append("\ncurrent value - ")
				.append(currentValue.getClass().getSimpleName())
				.append(' ')
				.append(currentValue.toString())
				.append("\nnew value - ")
				.append(value.getClass().getSimpleName())
				.append(' ')
				.append(value.toString());
			
			throw new IllegalArgumentException(builder.toString());
		}
	}

	private static String envVal(String key, String defaultValue) {
		String result = System.getenv(key);

		if ((result == null) ||
			(result.isBlank())) {

			return defaultValue;
		}

		return result;
	}

	private static List<String> envList(String key) {
		
		String json = System.getenv(key);

		if ((json == null) ||
			(json.isBlank())) {

			return Collections.emptyList();
		}
		
		try {
			return MapperUtil.jsonMapper.readValue(json, MapperUtil.LIST_REF);
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
	}
	
	private static Map<String, Object> envMap(String key) {
		String json = System.getenv(key);

		if ((json == null) ||
			(json.isBlank())) {

			return Collections.emptyMap();
		}

		try {
			return MapperUtil.jsonMapper.readValue(json, MapperUtil.MAP_REF);
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
	}
	
	private static String uuid() {
		return UUID.randomUUID().toString();
	}
}
