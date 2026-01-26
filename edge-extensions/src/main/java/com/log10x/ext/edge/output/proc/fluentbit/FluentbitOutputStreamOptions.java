package com.log10x.ext.edge.output.proc.fluentbit;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.edge.output.proc.ProcStreamOptions;

/*
 * A POJO used to parse the options used to configure {@link FluentbitOutputStream}.
 * To learn more about each individual option, see: 
 * {@link https://doc.log10x.com/run/output/event/fluentbit/}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FluentbitOutputStreamOptions implements ProcStreamOptions {
	
	public static final int DEFAULT_STARTUP_WAIT_MS = 5000;
	public static final String DEFAULT_STARTUP_WAIT_PATTERN = "\\[\\s*sp\\s*]\\s+stream\\s+processor\\s+started\\b";
	
	public final String fluentbitOutCommand;
	
	public final List<String> fluentbitOutInputArgs;
	
	public final List<String> fluentbitOutOutputArgs;

	public final boolean fluentbitOutLazyLaunch;
	
	public final long fluentbitOutStartupWaitMs;
	
	public final String fluentbitOutStartupWaitPattern;
	
	public final String fluentbitOutDestroyWait;
	
	public final int fluentbitOutMaxCacheSize;
	
	public final ProcDestroyMode fluentbitOutDestroyMode;
	
	private transient List<String> fluentbitArgs;
	private transient Pattern compiledStartupWaitPattern;

	protected FluentbitOutputStreamOptions() {
		this(null, new LinkedList<>(), new LinkedList<>(), false, DEFAULT_STARTUP_WAIT_MS,
				DEFAULT_STARTUP_WAIT_PATTERN, ProcDestroyMode.GREACEFUL, null, 0);
	}
	
	public FluentbitOutputStreamOptions(String fluentbitOutCommand,
		List<String> fluentbitOutInputArgs, List<String> fluentbitOutOutputArgs,
		boolean fluentbitOutLazyLaunch, int fluentbitOutStartupWaitMs,
		String fluentbitOutStartupWaitPattern, ProcDestroyMode fluentbitOutDestroyMode,
		String fluentbitOutDestroyWait, int fluentbitOutMaxCacheSize) {
		
		this.fluentbitOutCommand = fluentbitOutCommand;
		this.fluentbitOutInputArgs = fluentbitOutInputArgs;
		this.fluentbitOutOutputArgs = fluentbitOutOutputArgs;
		this.fluentbitOutLazyLaunch = fluentbitOutLazyLaunch;
		this.fluentbitOutStartupWaitMs = fluentbitOutStartupWaitMs;
		this.fluentbitOutStartupWaitPattern = fluentbitOutStartupWaitPattern;
		this.fluentbitOutDestroyMode = fluentbitOutDestroyMode;
		this.fluentbitOutDestroyWait = fluentbitOutDestroyWait;
		this.fluentbitOutMaxCacheSize = fluentbitOutMaxCacheSize;
	}

	@Override
	public String command() {
		return this.fluentbitOutCommand;
	}
	
	@Override
	public List<String> args() {

		if (this.fluentbitArgs != null) {
			return this.fluentbitArgs;
		}
		
		List<String> args = new ArrayList<>(this.fluentbitOutInputArgs.size() + this.fluentbitOutOutputArgs.size());
		
		args.addAll(this.fluentbitOutInputArgs);
		args.addAll(this.fluentbitOutOutputArgs);
		
		this.fluentbitArgs = args;
		
		return args;
	}

	@Override
	public boolean lazyLaunch() {
		return this.fluentbitOutLazyLaunch;
	}
	
	@Override
	public long startupWaitMs() {
		return this.fluentbitOutStartupWaitMs;
	}
	
	@Override
	public Pattern startupWaitPattern() {
		
		if (this.fluentbitOutStartupWaitPattern == null) {
			return null;
		}
		
		if (this.compiledStartupWaitPattern == null) {
			
			try {
				this.compiledStartupWaitPattern = Pattern.compile(this.fluentbitOutStartupWaitPattern);
			} catch (Exception e) {
				throw new IllegalStateException("error validating 'procOutStartupWaitPattern' pattern", e);
			}
		}
		
		return this.compiledStartupWaitPattern;
	}
	
	@Override
	public String destroyWait() {
		return this.fluentbitOutDestroyWait;
	}
	
	@Override
	public int maxCacheSize() {
		return this.fluentbitOutMaxCacheSize;
	}
	
	@Override
	public boolean allowMultipleStreams() {
		return false;
	}
	
	@Override
	public ProcDestroyMode destroyMode() {
		return this.fluentbitOutDestroyMode;
	}
	
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
}
