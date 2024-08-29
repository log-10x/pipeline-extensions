package com.log10x.ext.cloud.index.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.log10x.ext.edge.json.MapperUtil;

/**
 * A POJO class used for deserializing l1x Object timestamp, template hash
 * and var tokens from the input stream defined in:
 * {@link https://github.com/l1x-co/config/blob/main/pipelines/run/modules/output/cloud/index/stream.yaml}
 */
public class EncodedEventInput {
	
	/**
	 * original event text, can be used for debugging.
	 */
	public String text;
	
	/**
	 * index position in stream
	 */
	public long groupSequence;
		
	/**
	 * epoch timestamps
	 */
	public long[] timestamp;
	
	/**
	 * templates hash. See: '/run/units/structure/template/settings.yaml'
	 */
	public String templateHash;
	
	/**
	 * var tokens (i.e. tokens not found in template)
	 */
	public Object[] vars;
	
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
}
