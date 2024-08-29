package com.log10x.ext.edge.output.proc;

import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.ext.edge.json.MapperUtil;

/*
 * A POJO used to parse the options used to configure {@link ProcOutputStream}.
 * To learn more about each individual option, see: 
 * {@link http://doc.log10x.com/run/output/event/stream/process/}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcOutputStreamOptions {
	
	public static enum ProcDestroyMode {
		
		GREACEFUL,
		FORCIBLE,
		CACHED
	}
	
	public final String procOutCommand;
	
	public final List<String> procOutArgs;

	public final boolean procOutLazyLaunch;
	
	public final String procOutDestroyWait;
	
	public final int procOutMaxCacheSize;
	
	public final boolean procOutAllowMultipleStreams;

	public final ProcDestroyMode procOutDestroyMode;

	public ProcOutputStreamOptions(String procOutCommand,
		List<String> procOutArgs, boolean procOutLazyLaunch,
		ProcDestroyMode procOutDestroyMode, String procOutDestroyWait,
		int procOutMaxCacheSize, boolean procOutAllowMultipleStreams) {
		
		this.procOutCommand = procOutCommand;
		this.procOutArgs = procOutArgs;
		this.procOutLazyLaunch = procOutLazyLaunch;
		this.procOutDestroyMode = procOutDestroyMode;
		this.procOutDestroyWait = procOutDestroyWait;
		this.procOutMaxCacheSize = procOutMaxCacheSize;
		this.procOutAllowMultipleStreams = procOutAllowMultipleStreams;
	}

	protected ProcOutputStreamOptions() {
		this(null, new LinkedList<>(), false, ProcDestroyMode.GREACEFUL, null, 0, false);
	}

	public String command() {
		return this.procOutCommand + " " + this.procOutArgs;
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
