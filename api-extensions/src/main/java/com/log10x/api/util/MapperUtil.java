package com.log10x.api.util;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperBuilder;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.log10x.api.eval.EvaluableDeserializer;
import com.log10x.api.eval.EvaluableModule;
import com.log10x.api.eval.EvaluatorBean;

/**
 * A utility class for reading POJOs from JSON
 */
public class MapperUtil {

	public static final JsonMapper jsonMapper = initMapper(JsonMapper.builder());
	
	public static TypeReference<List<String>> LIST_REF = 
			new TypeReference<List<String>>() {};
	
	public static TypeReference<Map<String, Object>> MAP_REF = 
		new TypeReference<Map<String, Object>>() {};

	public static <M extends ObjectMapper, B extends MapperBuilder<M,B>> 
		M initMapper(MapperBuilder<M, B> builder) {
		
			return builder.
				visibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY).	
				configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true).
				enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).
				build();		
	}
	
	public static final JsonMapper noFailUnknownJsonMapper = JsonMapper.builder()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build();

	/**
	 * Creates a copy of the standard JSON mapper configured with an {@link EvaluatorBean}
	 * for automatic evaluation of {@link com.log10x.api.eval.Evaluable} annotated fields.
	 *
	 * Fields annotated with {@code @Evaluable} will have their string values passed
	 * through {@link EvaluatorBean#eval(String)} during deserialization.
	 *
	 * @param evaluator the evaluator bean to use for expression evaluation
	 * @return a configured ObjectMapper instance
	 */
	public static ObjectMapper jsonMapper(EvaluatorBean evaluator) {

		ObjectMapper mapper = jsonMapper.copy();
		mapper.registerModule(new EvaluableModule());

		InjectableValues.Std injectables = new InjectableValues.Std();
		injectables.addValue(EvaluableDeserializer.EVALUATOR_KEY, evaluator);
		mapper.setInjectableValues(injectables);

		return mapper;
	}
}
