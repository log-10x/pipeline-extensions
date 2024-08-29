package com.log10x.ext.cloud.compile.symbol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * This class demonstrates the expected JSON schema of a symbol unit 
 * emitted to the console by a program launched by an l1x 'compile' pipeline 'exec' scanner.
 * This method allows developers to extract symbolic information from an arbitrary
 * target source code/binary file or an external source (e.g. DB, web service).
 */
public class SymbolUnit { 
	
	/**
	 * The Jackson parser is used in this example to produce the output JSON,
	 * but any parser/language can be used.
	 */
	private static final ObjectMapper mapper = new ObjectMapper();
	
	/**
	 * The different contexts under which a a symbol can appear in
	 * a source/binary input file. To learn more, see:
	 * https://github.com/l1x-co/config/blob/main/pipelines/run/units/transform/symbol/options.yaml
	 */
	public enum SymbolSourceContext {
		
		EXECUTABLE_SYM,
		PLAIN_TEXT_SYM,	
		PACKAGE,
		CLASS,
		INTERFACE,
		ENUM,
		METHOD_DECL,			
		VAR_ASSIGN,
		METHOD_INVOKE,
		ANNOTATION_INVOKE,
	}
	
	/**
	 * A node within the symbol unit depicting a class, function, etc..
	 */
	public static class SymbolUnitNode {
		
		/**
		 * Symbol name (e.g. class/function name, code constant)
		 */
		public final String value;
		
		/**
		 * The context in which the symbol appears in the source/binary input
		 */
		public final SymbolSourceContext context;
		
		/**
		 * Sub-symbols for this node (e.g. functions under a class node)
		 */
		public final Collection<SymbolUnitNode> children;
		
		/**
		 * For use by a Jackson ObjectMapper
		 */
		protected SymbolUnitNode() {
			this(null, null);
		}
		
		public SymbolUnitNode(String value, SymbolSourceContext context) {
			
			this.value = value;
			this.context = context;  
			this.children = new ArrayList<>();
		}
	}
	
	public final Collection<SymbolUnitNode> nodes = new ArrayList<>();
			
	// Convenience methods:
	
	public SymbolUnit append(SymbolUnitNode node) {
		nodes.add(node);
		return this;
	}
	
	public SymbolUnit append(String value, SymbolSourceContext ctxt) {
		nodes.add(new SymbolUnitNode(value, ctxt));
		return this;
	}
	
	public SymbolUnit append(Map<String, SymbolSourceContext> values) {
		
		for (Entry<String, SymbolSourceContext> entry : values.entrySet()) {
			nodes.add(new SymbolUnitNode(entry.getKey(), entry.getValue()));
		}
		
		return this;
	}
	
	/*
	 * Renders a JSON object
	 */
	@Override
	public String toString() {
	
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return e.toString();
		}
	}
		
	/**
	 * A simple example program to emit a structured l1x symbol tree via its stdout.
	 * This can be implemented in any language.
	 * Each line written to stdout will be parsed as a symbol unit JSON object.
	 * 
	 * @param args	When launched by an l1x 'compile pipeline 
	 * 				this value contains the value set by the 'execArgs' startup argument 
	 * 				defined in XXX TODO
	 * 				The contents of this file can contain either the content to scan for symbols or
	 * 				configuration values used by this program (e.g., URLs, authentication tokens)
	 * 				used to fetch the content to scan.
	 */
	public static void main(String args[]) {
				
		System.out.println(new SymbolUnit().
			append("myEnum", SymbolSourceContext.ENUM).
				append(Map.of(
					"literal1", SymbolSourceContext.VAR_ASSIGN,
					"literal2", SymbolSourceContext.VAR_ASSIGN
				)
			)
		);
		
		// output:
		// {"nodes":[{"value":"myEnum","context":"ENUM","children":[]},{"value":"literal1","context":"VAR_ASSIGN","children":[]},{"value":"literal2","context":"VAR_ASSIGN","children":[]}]} 
	}
}