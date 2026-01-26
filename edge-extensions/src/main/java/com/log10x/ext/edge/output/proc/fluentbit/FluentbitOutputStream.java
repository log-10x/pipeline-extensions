package com.log10x.ext.edge.output.proc.fluentbit;

import java.util.Map;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.edge.output.proc.ProcOutputStream;
import com.log10x.ext.edge.output.proc.ProcStreamOptions;

public class FluentbitOutputStream extends ProcOutputStream {

	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the 10x cli for the
	 * `		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this output
	 */
	public FluentbitOutputStream(Map<String, Object> args, EvaluatorBean evaluatorBean) {
		
		super(MapperUtil.jsonMapper.convertValue(args,
			FluentbitOutputStreamOptions.class), evaluatorBean);
	}
	
	@Override
	protected void validateOptions(ProcStreamOptions options) {
		
		if (options.startupWaitMs() < 0) {
			
			throw new IllegalArgumentException("'fluentbitOutStartupWaitMs' can't be negative - " + options);
		}
		
		if (options.startupWaitMs() > MAX_STARTUP_WAIT_MS) {
			
			throw new IllegalArgumentException("'fluentbitOutStartupWaitMs' can't be grater than " + MAX_STARTUP_WAIT_MS + " - " + options);
		}
		
		super.validateOptions(options);
	}
}
