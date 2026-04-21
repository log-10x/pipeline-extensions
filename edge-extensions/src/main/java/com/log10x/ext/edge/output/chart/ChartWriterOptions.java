package com.log10x.ext.edge.output.chart;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.util.MapperUtil;

/*
 * A POJO used to parse the options used to configure {@link ChartOutputStream}.
 * To learn more about each individual option, see: 
 * {@link http://doc.log10x.com/run/output/event/stream/chart/}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChartWriterOptions {

	public final String name;

	public final List<String> outputStreamFields;
	private final String chartsStr;

	public final int rows;
	public final int width;

	public final String nameField;

	private transient List<String> charts;
	
	public ChartWriterOptions(String name, List<String> outputStreamFields,
			String chartsStr, int rows, int width, String nameField) {

		this.name = name;
		this.outputStreamFields = outputStreamFields;
		this.chartsStr = chartsStr;
		this.rows = rows;
		this.width = width;
		this.nameField = nameField;
	}

	protected ChartWriterOptions() {
		this(null, new LinkedList<>(), null, 0, 0, null);
	}

	public synchronized List<String> charts() {
		
		if (this.charts == null) {
			this.charts = Arrays.asList(this.chartsStr.split(","));
		}
		
		return this.charts;
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
