package com.log10x.api.eval;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

/**
 * A Jackson deserializer that evaluates string values through {@link EvaluatorBean}
 * and converts the result to the target type.
 *
 * This deserializer is activated for fields annotated with {@link Evaluable}.
 * It reads the JSON value as a string, passes it through {@link EvaluatorBean#eval(String)},
 * and converts the result to the target field type.
 *
 * The {@link EvaluatorBean} must be provided via Jackson's injectable values mechanism.
 * Use {@link com.log10x.api.util.MapperUtil#jsonMapper(EvaluatorBean)} to obtain
 * a properly configured ObjectMapper.
 */
public class EvaluableDeserializer extends JsonDeserializer<Object>
		implements ContextualDeserializer {

	public static final String EVALUATOR_KEY = EvaluatorBean.class.getName();

	private final JavaType targetType;

	public EvaluableDeserializer() {
		this.targetType = null;
	}

	private EvaluableDeserializer(JavaType targetType) {
		this.targetType = targetType;
	}

	@Override
	public JsonDeserializer<?> createContextual(DeserializationContext ctxt,
			BeanProperty property) {

		if (property != null) {
			return new EvaluableDeserializer(property.getType());
		}

		return this;
	}

	@Override
	public Object deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException {

		String rawValue = p.getValueAsString();

		if (rawValue == null) {
			return null;
		}

		EvaluatorBean evaluator = (EvaluatorBean) ctxt.findInjectableValue(
				EVALUATOR_KEY, null, null);

		if (evaluator == null) {
			// No evaluator available, try to convert raw value to target type
			return convertToTargetType(rawValue, ctxt);
		}

		Object evaluated = evaluator.eval(rawValue);

		if (evaluated == null) {
			return null;
		}

		// If the evaluated result is already the target type, return it
		if (targetType != null && targetType.getRawClass().isInstance(evaluated)) {
			return evaluated;
		}

		// Convert the evaluated result to the target type
		return convertToTargetType(evaluated, ctxt);
	}

	private Object convertToTargetType(Object value, DeserializationContext ctxt) {

		if (targetType == null) {
			return value;
		}

		Class<?> rawClass = targetType.getRawClass();

		// Handle primitive types and their wrappers directly
		if (rawClass == String.class) {
			return value.toString();
		}

		if (rawClass == long.class || rawClass == Long.class) {
			if (value instanceof Number) {
				return ((Number) value).longValue();
			}
			return Long.parseLong(value.toString());
		}

		if (rawClass == int.class || rawClass == Integer.class) {
			if (value instanceof Number) {
				return ((Number) value).intValue();
			}
			return Integer.parseInt(value.toString());
		}

		if (rawClass == double.class || rawClass == Double.class) {
			if (value instanceof Number) {
				return ((Number) value).doubleValue();
			}
			return Double.parseDouble(value.toString());
		}

		if (rawClass == float.class || rawClass == Float.class) {
			if (value instanceof Number) {
				return ((Number) value).floatValue();
			}
			return Float.parseFloat(value.toString());
		}

		if (rawClass == boolean.class || rawClass == Boolean.class) {
			if (value instanceof Boolean) {
				return value;
			}
			return Boolean.parseBoolean(value.toString());
		}

		if (rawClass == short.class || rawClass == Short.class) {
			if (value instanceof Number) {
				return ((Number) value).shortValue();
			}
			return Short.parseShort(value.toString());
		}

		if (rawClass == byte.class || rawClass == Byte.class) {
			if (value instanceof Number) {
				return ((Number) value).byteValue();
			}
			return Byte.parseByte(value.toString());
		}

		// For complex types, use ObjectMapper's convertValue
		ObjectMapper mapper = (ObjectMapper) ctxt.getParser().getCodec();
		return mapper.convertValue(value, targetType);
	}
}
