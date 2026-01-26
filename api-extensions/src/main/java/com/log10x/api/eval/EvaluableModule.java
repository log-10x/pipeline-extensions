package com.log10x.api.eval;

import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A Jackson module that enables automatic evaluation of {@link Evaluable}
 * annotated fields during deserialization.
 *
 * This module uses a custom {@link com.fasterxml.jackson.databind.AnnotationIntrospector}
 * to detect {@code @Evaluable} annotations and apply the {@link EvaluableDeserializer}.
 * When the ObjectMapper has an {@link EvaluatorBean} injected, annotated fields will
 * have their JSON string values passed through {@link EvaluatorBean#eval(String)}
 * before type conversion.
 *
 * This works for any field type (String, long, int, etc.) as long as the JSON
 * value is a string that can be evaluated.
 *
 * @see com.log10x.api.util.MapperUtil#jsonMapper(EvaluatorBean)
 */
public class EvaluableModule extends SimpleModule {

	private static final long serialVersionUID = 1L;

	public EvaluableModule() {
		super("EvaluableModule");
	}

	@Override
	public void setupModule(SetupContext context) {
		super.setupModule(context);

		context.insertAnnotationIntrospector(new NopAnnotationIntrospector() {

			private static final long serialVersionUID = 1L;

			@Override
			public Object findDeserializer(Annotated am) {

				if (am.hasAnnotation(Evaluable.class)) {
					return EvaluableDeserializer.class;
				}

				return null;
			}
		});
	}
}
