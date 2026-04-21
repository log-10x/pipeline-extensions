package com.log10x.ext.edge.output.chart;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.edge.util.StringBuilderReader;

public class ChartWriter extends Writer {
	
	private static final Logger logger = LogManager.getLogger(ChartWriter.class);
	
	private static final PrintStream OUT = System.out;
	
	protected final ChartWriterOptions options;
	
	protected final StringBuilderReader currChars;
	
	protected final List<Map<String, Object>> summaries;
	
	private volatile boolean closed;
	
	/**
	 * this constructor is invoked by the 10x run-time.
	 * 
	 * @param 	args
	 * 			a map arguments of arguments passed to the 10x cli for the
	 *  		target output for which this stream is instantiated
	 * 
	 * @param 	evaluatorBean
	 * 			a reference to an 10x evaluator bean which allows this output
	 */
	public ChartWriter(Map<String, Object> args, EvaluatorBean evaluatorBean) {
		
		this(MapperUtil.jsonMapper.convertValue(args, ChartWriterOptions.class));
	}
	
	public ChartWriter(ChartWriterOptions options) {
		
		this.options = options;
		
		this.currChars = new StringBuilderReader();
		this.summaries = new ArrayList<>();
		
		if (options.outputStreamFields.isEmpty()) {
			throw new IllegalArgumentException("chartOutFields can't be empty - " + options);
		}
		
		if (options.charts().isEmpty()) {
			throw new IllegalArgumentException("chartOutCharts can't be empty - " + options);
		}
		
		if ((options.nameField == null) || (options.nameField.isBlank())) {
			throw new IllegalArgumentException("chartOutNameField can't be empty - " + options);
		}
		
		if (options.rows < 1) {
			throw new IllegalArgumentException("chartOutRows must be positive - " + options);
		}
		
		if (options.width < 1) {
			throw new IllegalArgumentException("chartOutWidth must be positive - " + options);
		}
		
		for (String chart : options.charts()) {
			if (!options.outputStreamFields.contains(chart)) {
				throw new IllegalArgumentException("chartOutFields missing value of chart '" + chart + "' - " + options);
			}
		}
		
		if (!options.outputStreamFields.contains(options.nameField)) {
			throw new IllegalArgumentException("chartOutFields missing value of name field '" + options.nameField + "' - " + options);
		}
	}

	@Override
	public synchronized void write(char[] cbuf, int off, int len) throws IOException {
		currChars.builder.append(cbuf, off, len);	
	}

	@Override
	public synchronized void flush() throws IOException {

		if (this.closed) {
			throw new IllegalStateException("closed");
		}
		
		if (currChars.builder.isEmpty()) {
			return;
		}
		
		try {
		
			Map<String, Object> summary = new HashMap<>();
			
			MapperUtil.jsonMapper.
				readerForUpdating(summary).
				readValue(this.currChars);
			
			this.summaries.add(summary);
				
		} catch (Exception e) {
			
			logger.error("error flushing: " + this.currChars, e);
			
		} finally { 
			
			currChars.reset();
		}
	}

	@Override
	public synchronized void close() throws IOException {
		
		if (this.closed) {
			throw new IOException("closed");
		}
		
		this.closed = true;
		
		for (String chart : options.charts()) {
			try {
				doChart(chart);
			} catch (Exception e) {
				logger.error("Failed charting for {} - {}", this.options.name, chart, e);
			}
		}
	}

	private void doChart(String chart) {
		
		List<Map<String, Object>> filtered = this.summaries.stream().filter(summary -> {
			Object o = summary.get(chart);
			
			if (!(o instanceof Number)) {
				return false;
			}
			
			return true;
		}).collect(Collectors.toList());
		
		long totalValue = filtered.stream().mapToLong(summary -> value(summary, chart)).sum();
		
		List<Map<String, Object>> topItems = filtered.stream().sorted(new Comparator<Map<String, Object>>() {

			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				long val1 = value(o1, chart);
				long val2 = value(o2, chart);
				
				long diff = val2 - val1;
				
				return (int)diff;
			}
		}).limit(this.options.rows).collect(Collectors.toList());
		
		chartHeader(chart, totalValue);
		
		int maxIndexDigits = String.valueOf(Math.abs(topItems.size())).length();
		
		for (int i = 0; i < topItems.size(); i++) {
			Map<String, Object> topItem = topItems.get(i);
			
			chartLine((i + 1), topItem, chart, totalValue, maxIndexDigits);
		}
		
		contributersHeader(chart);
		
		for (int i = 0; i < topItems.size(); i++) {
			Map<String, Object> topItem = topItems.get(i);
			
			chartContributer((i + 1), topItem, maxIndexDigits);
		}
	}

	private void chartHeader(String chart, long totalValue) {
		
		OUT.println(String.format("\n%s - %s - total value of %d\n", this.options.name, chart, totalValue));
	}
	
	private void chartLine(int index, Map<String, Object> topItem, String chart, long totalValue, int maxIndex) {
		
		long value = value(topItem, chart);
		int barLength = (int)((this.options.width * value) / totalValue);
		
		String formattedIndex = String.format("%-" + maxIndex + "d", index);
		String bar = "#".repeat(barLength);
		
		OUT.println(String.format("%s %s - %d (%d%%)", formattedIndex, bar, value, (value * 100) / totalValue));
	}
	
	private static void contributersHeader(String chart) {

		OUT.println(String.format("\nTop contributers for %s -", chart));
	}
	
	private void chartContributer(int index, Map<String, Object> topItem, int maxIndex) {
		
		String formattedIndex = String.format("%-" + maxIndex + "d", index);
		
		OUT.println(String.format("%s %s", formattedIndex, name(topItem)));
	}
	
	private String name(Map<String, Object> summary) {
		return (summary.get(this.options.nameField).toString());
	}
	
	private static long value(Map<String, Object> summary, String chart) {
		return ((Number)summary.get(chart)).longValue();
	}
}
