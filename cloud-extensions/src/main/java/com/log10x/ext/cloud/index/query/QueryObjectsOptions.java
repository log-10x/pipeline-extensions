package com.log10x.ext.cloud.index.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines a list of serializable POJO requests to fetch a set of byte ranges
 * from an input object residing within a KV storage, and extract events
 * falling within a time range and matching a set of search terms.
 * To learn more more, see:
 * {@link https://github.com/log-10x/config/blob/main/pipelines/run/modules/input/index/object/options.yaml }

 */
public class QueryObjectsOptions {

	public final List<QueryObjectOptions> queryObject;
	
	public QueryObjectsOptions() {
		this(new ArrayList<>());
	}
	
	public QueryObjectsOptions(List<QueryObjectOptions> queryObject) {
		this.queryObject = queryObject;
	}
}
