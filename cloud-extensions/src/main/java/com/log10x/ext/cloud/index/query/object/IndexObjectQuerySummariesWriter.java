package com.log10x.ext.cloud.index.query.object;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import com.log10x.api.eval.EvaluatorBean;
import com.log10x.api.util.MapperUtil;
import com.log10x.ext.cloud.index.interfaces.ObjectStorageIndexAccessor.IndexObjectType;

/**
 * Sibling of {@link IndexObjectQueryResultsWriter} that uploads to the
 * {@code querySummaries} (qrs/) prefix instead of {@code queryResults}
 * (qr/). Behavior is otherwise identical — same JSONL buffering, same
 * slice-keyed S3 path, same per-worker cap and truncation marker.
 *
 * Differentiated as a subclass rather than via a writer-block arg because
 * the engine's option-flattening rejects nested {@code args:} maps in
 * stream.yaml, and the per-block {@code path:} is the only knob we have
 * to route different filtered streams (TenXObject vs TenXSummary) to
 * different S3 prefixes from a single pipeline output stage.
 */
public class IndexObjectQuerySummariesWriter extends IndexObjectQueryResultsWriter {

	public IndexObjectQuerySummariesWriter(Map<String, Object> args, EvaluatorBean evaluatorBean)
		throws NoSuchAlgorithmException, IllegalArgumentException, IOException {

		super(MapperUtil.jsonMapper.convertValue(args, IndexQueryObjectOptions.class),
			IndexObjectType.querySummaries, null, evaluatorBean);
	}
}
