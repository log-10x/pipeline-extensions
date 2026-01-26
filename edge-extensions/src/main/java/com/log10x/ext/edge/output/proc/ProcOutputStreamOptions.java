package com.log10x.ext.edge.output.proc;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.util.MapperUtil;

/*
 * A POJO used to parse the options used to configure {@link ProcOutputStream}.
 * To learn more about each individual option, see: 
 * {@link https://doc.log10x.com/run/output/event/process/}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcOutputStreamOptions implements ProcStreamOptions {
	
	public final String procOutCommand;
	
	public final List<String> procOutArgs;

	public final boolean procOutLazyLaunch;
	
	public final long procOutStartupWaitMs;
	
	public final String procOutStartupWaitPattern;
	
	public final String procOutDestroyWait;
	
	public final int procOutMaxCacheSize;
	
	public final boolean procOutAllowMultipleStreams;

	public final ProcDestroyMode procOutDestroyMode;
	
	private transient Pattern compiledStartupWaitPattern;

	protected ProcOutputStreamOptions() {
		this(null, new LinkedList<>(), false, 0, null, ProcDestroyMode.GREACEFUL, null, 0, false);
	}
	
	public ProcOutputStreamOptions(String procOutCommand,
		List<String> procOutArgs, boolean procOutLazyLaunch,
		int procOutStartupWaitMs, String procOutStartupWaitPattern,
		ProcDestroyMode procOutDestroyMode, String procOutDestroyWait,
		int procOutMaxCacheSize, boolean procOutAllowMultipleStreams) {
		
		this.procOutCommand = procOutCommand;
		this.procOutArgs = procOutArgs;
		this.procOutLazyLaunch = procOutLazyLaunch;
		this.procOutStartupWaitMs = procOutStartupWaitMs;
		this.procOutStartupWaitPattern = procOutStartupWaitPattern;
		this.procOutDestroyMode = procOutDestroyMode;
		this.procOutDestroyWait = procOutDestroyWait;
		this.procOutMaxCacheSize = procOutMaxCacheSize;
		this.procOutAllowMultipleStreams = procOutAllowMultipleStreams;
	}

	@Override
	public String command() {
		return this.procOutCommand;
	}
	
	@Override
	public List<String> args() {
		return this.procOutArgs;
	}

	@Override
	public boolean lazyLaunch() {
		return this.procOutLazyLaunch;
	}
	
	@Override
	public long startupWaitMs() {
		return this.procOutStartupWaitMs;
	}
	
	@Override
	public Pattern startupWaitPattern() {
		
		if (this.procOutStartupWaitPattern == null) {
			return null;
		}
		
		if (this.compiledStartupWaitPattern == null) {
			
			try {
				this.compiledStartupWaitPattern = Pattern.compile(this.procOutStartupWaitPattern);
			} catch (Exception e) {
				throw new IllegalStateException("error validating 'procOutStartupWaitPattern' pattern", e);
			}
		}
		
		return this.compiledStartupWaitPattern;
	}
	
	@Override
	public String destroyWait() {
		return this.procOutDestroyWait;
	}
	
	@Override
	public int maxCacheSize() {
		return this.procOutMaxCacheSize;
	}
	
	@Override
	public boolean allowMultipleStreams() {
		return this.procOutAllowMultipleStreams;
	}
	
	@Override
	public ProcDestroyMode destroyMode() {
		return this.procOutDestroyMode;
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
