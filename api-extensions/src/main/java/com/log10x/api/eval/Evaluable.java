package com.log10x.api.eval;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a POJO field for automatic evaluation during JSON deserialization.
 *
 * When a field is annotated with {@code @Evaluable}, the JSON string value
 * will be passed through {@link EvaluatorBean#eval(String)} before assignment.
 *
 * This requires using {@link com.log10x.api.util.MapperUtil#jsonMapper(EvaluatorBean)}
 * to obtain an ObjectMapper configured with the evaluator.
 *
 * Example:
 * <pre>
 * public class MyOptions {
 *     @Evaluable
 *     public final String dynamicValue;
 * }
 * </pre>
 *
 * If the JSON contains {@code {"dynamicValue": "env.MY_VAR"}}, the field will
 * be set to the result of {@code evaluatorBean.eval("env.MY_VAR")}.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Evaluable {
}
