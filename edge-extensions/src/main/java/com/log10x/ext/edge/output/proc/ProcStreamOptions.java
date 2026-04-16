package com.log10x.ext.edge.output.proc;

import java.util.List;
import java.util.regex.Pattern;

public interface ProcStreamOptions {

	public static enum ProcDestroyMode {	
		GREACEFUL,
		FORCIBLE,
		CACHED
	}
	
	public String command();
	
	public List<String> args();
	
	public default String fullCommand() {
		return command() + " " + args();
	}
	
	public boolean lazyLaunch();
	
	public long startupWaitMs();
	
	public Pattern startupWaitPattern();
	
	public String destroyWait();
	
	public int maxCacheSize();
	
	public boolean allowMultipleStreams();
	
	public ProcDestroyMode destroyMode();
}
