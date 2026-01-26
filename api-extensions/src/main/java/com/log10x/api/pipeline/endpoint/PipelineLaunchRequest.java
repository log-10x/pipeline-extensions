package com.log10x.api.pipeline.endpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.pipeline.launch.PipelineLaunchOptions;
import com.log10x.api.util.MapperUtil;

/**
 * A utility class used to generate the JSON request body for
 * launching a remote 10x pipeline accessible via a REST endpoint. 
 */
public class PipelineLaunchRequest {

	public static final String TENX_APP		= "TENX_DEFAULT_APP";
	public static final String BOOTSTRAP	= "TENX_DEFUALT_BOOTSTRAP";
	public static final String CONFIG		= "TENX_DEFAULT_CONFIG";
	
	/**
	 * List of bootstrap launch arguments to pass to the 10x engine.
	 * For more details see - https://doc.log10x.com/run/bootstrap/
	 */
	public final List<String> bootstrapArgs;
	
	/**
	 * Type of 10x app to launch, i.e '@apps/cloud/reporter', etc.
	 */
	public final String tenx;
		
	/**
	 * Configuration to pass into the launched pipeline.
	 * This will be formatted as JSON and passed using https://doc.log10x.com/config/json/
	 */
	public final Map<String, Object> pipelineConfig;
	
	private PipelineLaunchRequest() {

		this(Collections.emptyList(), null, Collections.emptyMap());
	}
	
	private PipelineLaunchRequest(List<String> bootstrapArgs, String tenx, Map<String, Object> pipelineConfig) {

		this.bootstrapArgs = bootstrapArgs;
		this.tenx = tenx;
		this.pipelineConfig = pipelineConfig;
	}
	
	public PipelineLaunchRequest merge(Object... values) {
		return this.doMerge(false, values);
	}
	
	public PipelineLaunchRequest overwrite(Object... values) {
		return this.doMerge(true, values);
	}
	
	private PipelineLaunchRequest doMerge(boolean overwrite, Object... values) {
	
		Builder builder = this.toBuilder();
		
		for (Object value : values) {
			builder.with(value, overwrite);
		}
		
		return builder.build();
	}
	
	public String[] asPipelineArgs() throws JsonProcessingException {
		
		List<String> allArgs = new LinkedList<>();
		
		if (!this.bootstrapArgs.isEmpty()) {
			allArgs.addAll(this.bootstrapArgs);
		}
		
		if ((this.tenx != null) && (!this.tenx.isBlank())) {
			allArgs.add(this.tenx);
		} else {
			allArgs.add(PipelineLaunchOptions.RUN_PIPELINE);
		}
		
		if (!this.pipelineConfig.isEmpty()) {
			
			String configAsJson = MapperUtil.jsonMapper.writeValueAsString(this.pipelineConfig);		
			allArgs.add(PipelineLaunchOptions.MACRO_PREFIX + configAsJson);
		}
		
		return allArgs.toArray(new String[0]);
	}

	public String shortDesc() {
		StringBuilder builder = new StringBuilder();

		builder.append("Tenx: ");
		builder.append(this.tenx);
		builder.append(", ");
		builder.append(PipelineLaunchOptions.RUN_TIME_NAME);
		builder.append(": ");
		builder.append(runtimeName());

		return builder.toString();
	}

	public String runtimeName() {
		return configAttr(PipelineLaunchOptions.RUN_TIME_NAME, "Unknown").toString();
	}

	private Object configAttr(String name, Object defaultValue) {
		if (this.pipelineConfig == null) {
			return defaultValue;
		}

		Object result = this.pipelineConfig.get(name);

		return (result == null ? defaultValue : result);
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
		return new Builder(this.bootstrapArgs, this.tenx, this.pipelineConfig);
	}
	
	public static PipelineLaunchRequest from(Map<String, Object> map) {
		return MapperUtil.noFailUnknownJsonMapper.convertValue(map, PipelineLaunchRequest.class);
	}

	/**
	 * @return A new PipelineLaunchRequest.Builder from default values in environment
	 *         variables
	 */
	public static PipelineLaunchRequest.Builder fromEnv() {

		String tenx = envVal(TENX_APP, null);

		List<String> bootstrap = envList(BOOTSTRAP);
		Map<String, Object> options = envMap(CONFIG);

		return newBuilder()
				.withTenX(tenx)
				.withBootstrapArgs(bootstrap)
				.withConfig(options, true);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {

		private final List<String> bootstrapArgs;
		private String tenx;
		private final Map<String, Object> pipelineConfig;

		Builder() {
			this.bootstrapArgs = new LinkedList<>();
			this.tenx = null;
			this.pipelineConfig = new LinkedHashMap<>();
		}

		Builder(List<String> bootstrapArgs, String tenx, Map<String, Object> pipelineConfig) {

			this.bootstrapArgs = new LinkedList<>(bootstrapArgs);
			this.tenx = tenx;
			this.pipelineConfig = new LinkedHashMap<>(pipelineConfig);
		}

		public Builder withBootstrapArg(String arg, String value) {
			
			this.bootstrapArgs.add(arg);
			this.bootstrapArgs.add(value);
			
			return this;
		}
		
		public Builder withBootstrapArgs(List<String> args) {
			
			this.bootstrapArgs.addAll(args);
			
			return this;
		}
		
		public Builder withTenX(String tenx) {

			this.tenx = tenx;

			return this;
		}

		public Builder withArg(String key, Object value) {
			this.pipelineConfig.put(key, value);

			return this;
		}
		
		public Builder withOverride(String key, Object value) {
			
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> overrides = (List<Map<String, Object>>) this.pipelineConfig.get(PipelineLaunchOptions.OVERRIDE);
			
			if (overrides == null) {
				overrides = new LinkedList<Map<String,Object>>();
				this.pipelineConfig.put(PipelineLaunchOptions.OVERRIDE, overrides);
			}
			
			overrides.add(Map.of(PipelineLaunchOptions.OVERRIDE_KEY, key, PipelineLaunchOptions.OVERRIDE_VALUE, value));
			
			return this;
		}

		public Builder withIncludes(String... includes) {

			List<Map<String, Object>> headerIncludes = new ArrayList<>(includes.length);

			for (String include : includes) {
				headerIncludes.add(Collections.singletonMap(PipelineLaunchOptions.INCLUDE_SOURCE, include));
			}

			Map<String, Object> includesMap = Collections.singletonMap(PipelineLaunchOptions.INCLUDE, headerIncludes);

			mergeMaps(this.pipelineConfig, includesMap, false);

			return this;
		}

		public Builder withConfig(Map<String, Object> config, boolean overwrite) {

			mergeMaps(this.pipelineConfig, config, overwrite);

			return this;
		}

		public Builder with(Object value, boolean overwrite) {

			if (value == null) {
				return this;
			}

			if (value instanceof PipelineLaunchRequest) {

				PipelineLaunchRequest other = (PipelineLaunchRequest) value;

				if ((this.tenx == null) || (this.tenx.isBlank())) {
					this.tenx = other.tenx;
				} else if (!this.tenx.equals(other.tenx)) {
					throw new IllegalArgumentException("Can't merge launch requests from two pipeline types:\n" + this + "\n" + other);
				}

				this.withBootstrapArgs(other.bootstrapArgs);
				this.withConfig(other.pipelineConfig, overwrite);

			} else if (value instanceof PipelineLaunchRequest.Builder) {

				PipelineLaunchRequest.Builder other = (PipelineLaunchRequest.Builder) value;

				if ((this.tenx == null) || (this.tenx.isBlank())) {
					this.tenx = other.tenx;
				} else if ((other.tenx != null) && (!other.tenx.isBlank()) && (!this.tenx.equals(other.tenx))) {
					throw new IllegalArgumentException("Can't merge launch requests from two pipeline types:\n" + this + "\n" + other);
				}
				
				this.withBootstrapArgs(other.bootstrapArgs);
				this.withConfig(other.pipelineConfig, overwrite);

			} else if (value instanceof Map<?, ?>) {

				@SuppressWarnings("unchecked")
				Map<String, Object> other = (Map<String, Object>) value;

				this.withConfig(other, overwrite);

			} else if (value instanceof Object[]) {
				Object[] other = (Object[]) value;
				
				for (Object sub : other) {
					this.with(sub, overwrite);
				}
				
			} else {

				Map<String, Object> other = MapperUtil.jsonMapper.convertValue(value, MapperUtil.MAP_REF);

				this.withConfig(other, overwrite);
			}

			return this;
		}

		public PipelineLaunchRequest build() {
			return new PipelineLaunchRequest(this.bootstrapArgs, this.tenx, this.pipelineConfig);
		}
	}

	@SuppressWarnings("unchecked")
	private static void mergeMaps(Map<String, Object> to, Map<String, Object> from, 
		boolean overwrite) {

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

				Collection<Object> currentCollection = new ArrayList<>((List<Object>) currentValue);
				currentCollection.addAll((Collection<Object>) value);

				to.put(key, currentCollection);

				continue;
			}

			if ((value instanceof Map) &&
				(currentValue instanceof Map)) {

				Map<String, Object> currentMap = new LinkedHashMap<>((Map<String, Object>) currentValue);
				Map<String, Object> map = (Map<String, Object>) value;

				mergeMaps(currentMap, map, overwrite);

				to.put(key, currentMap);

				continue;
			}

			if (value.getClass() == currentValue.getClass()) {

				if (overwrite) {
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
}
