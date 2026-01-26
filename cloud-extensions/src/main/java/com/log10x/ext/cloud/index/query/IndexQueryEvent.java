package com.log10x.ext.cloud.index.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.api.util.MapperUtil;

/**
 * A POJO class used for deserializing tenxObject fields from the input stream defined in:
 * {@link https://github.com/log-10x/modules/blob/main/pipelines/run/modules/input/objectStorage/query/stream.yaml}
 */
public class IndexQueryEvent {

	/**
	 * original event text, can be used for debugging.
	 */
	public String text;

	/**
	 * TenXTemplate hash, see https://doc.log10x.com/api/js/#TenXBaseObject+templateHash
	 */
	public String templateHash;

	/**
	 * var tokens, see https://doc.log10x.com/api/js/#TenXBaseObject+vars
	 */
	public Object[] vars;

	/**
	 * enrichment fields values, see https://doc.log10x.com/run/initialize/#enrichmentfields
	 */
	public String[] enrichmentValues;

	/*
	 * Renders a JSON object
	 */
	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();

		builder.append(this.getClass().getSimpleName());
		builder.append(safeToJson());

		return builder.toString();
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
	
	/**
	 * Nullifies all fields of this class.
	 * Useful as this class is often reused and re-read using jackson.
	 */
	public void nullify() {
		this.text = null;
		this.templateHash = null;
		this.vars = null;
		this.enrichmentValues = null;
	}
}
