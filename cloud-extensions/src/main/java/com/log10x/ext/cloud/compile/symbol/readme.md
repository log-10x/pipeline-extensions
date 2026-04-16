# Symbol Module — Source Code Symbol Extraction

Schema and utilities for compile pipeline symbol extraction. Enables developers to extract symbolic information (class names, method signatures, variable assignments) from source code or binaries.

## Overview

Defines JSON schema for symbol units extracted from source code or binary files:

- **Symbol Schema** — Standardized JSON representation of code symbols
- **Context Types** — Categorization of symbol origins (executable, class, method, etc.)
- **Extensibility** — Custom source implementations
- **Jackson Integration** — JSON serialization/deserialization

## Use Case

Symbol extraction pipeline:
```
Source Code/Binary
    ↓
Scanner (user implementation)
    ↓
SymbolUnit (JSON serialization)
    ↓
Cloud Storage / Index
    ↓
Query Results (with code context)
```

**Result:** Query results enriched with source code context and stack trace information.

## Core Classes

### SymbolUnit
**Main container** for extracted symbol information

```java
public class SymbolUnit {
    private String id;              // Unique identifier
    private String name;            // Symbol name
    private SymbolSourceContext sourceContext;  // Origin context
    private List<SymbolUnitNode> nodes;  // Symbol tree
    private Map<String, Object> metadata;  // Custom data
}
```

### SymbolSourceContext
**Enum** categorizing symbol origins

```java
public enum SymbolSourceContext {
    EXECUTABLE_SYM,    // Executable symbol (function, method)
    PLAIN_TEXT_SYM,    // Plain text match
    CLASS,             // Java class definition
    METHOD_DECL,       // Method declaration
    VAR_ASSIGN,        // Variable assignment
    IMPORT_STMT,       // Import statement
    ANNOTATION,        // Annotation
    INTERFACE,         // Interface definition
    ENUM_DEF,          // Enumeration
    LAMBDA             // Lambda expression
}
```

### SymbolUnitNode
**Tree node** representing symbol hierarchy

```java
public class SymbolUnitNode {
    private String name;
    private String type;
    private int lineNumber;
    private String content;
    private List<SymbolUnitNode> children;
}
```

## JSON Schema

### Example Symbol Unit

```json
{
  "id": "com.log10x.Engine.process",
  "name": "process",
  "sourceContext": "METHOD_DECL",
  "nodes": [
    {
      "name": "Engine",
      "type": "class",
      "lineNumber": 42,
      "content": "public class Engine { ... }",
      "children": [
        {
          "name": "process",
          "type": "method",
          "lineNumber": 50,
          "content": "public void process(Event event) { ... }",
          "children": [
            {
              "name": "result",
              "type": "variable",
              "lineNumber": 52,
              "content": "String result = event.toString();"
            }
          ]
        }
      ]
    }
  ],
  "metadata": {
    "file": "Engine.java",
    "package": "com.log10x",
    "language": "java",
    "version": "1.0"
  }
}
```

## Implementation

### Basic Symbol Extractor

```java
public class MySymbolExtractor {
    private final ObjectMapper mapper = new ObjectMapper();
    
    public SymbolUnit extractSymbols(String sourceCode) {
        SymbolUnit unit = new SymbolUnit();
        unit.setId(generateId());
        unit.setName(extractClassName(sourceCode));
        unit.setSourceContext(SymbolSourceContext.CLASS);
        
        List<SymbolUnitNode> nodes = parseNodes(sourceCode);
        unit.setNodes(nodes);
        
        return unit;
    }
    
    public String toJson(SymbolUnit unit) throws IOException {
        return mapper.writeValueAsString(unit);
    }
}
```

### Java AST Parser Example

```java
public class JavaSymbolExtractor {
    public SymbolUnit extractFromJavaSource(File file) {
        CompilationUnit cu = StaticJavaParser.parse(file);
        
        SymbolUnit unit = new SymbolUnit();
        unit.setId(cu.getPackageDeclaration()
            .map(pd -> pd.getNameAsString())
            .orElse("") + "." + extractClassName(cu));
        
        List<SymbolUnitNode> nodes = cu.getTypes()
            .stream()
            .map(this::parseType)
            .collect(Collectors.toList());
        
        unit.setNodes(nodes);
        return unit;
    }
    
    private SymbolUnitNode parseType(TypeDeclaration<?> type) {
        SymbolUnitNode node = new SymbolUnitNode();
        node.setName(type.getNameAsString());
        node.setType(type.isClassOrInterfaceDeclaration() ? 
                     "class" : "interface");
        node.setLineNumber(type.getBegin()
            .map(Range::getBeginLine).orElse(0));
        
        List<SymbolUnitNode> children = type.getMembers()
            .stream()
            .map(this::parseMember)
            .collect(Collectors.toList());
        
        node.setChildren(children);
        return node;
    }
}
```

### Binary Symbol Extraction

```java
public class BinarySymbolExtractor {
    public SymbolUnit extractFromClass(String className) {
        Class<?> clazz = loadClass(className);
        
        SymbolUnit unit = new SymbolUnit();
        unit.setId(clazz.getCanonicalName());
        unit.setName(clazz.getSimpleName());
        unit.setSourceContext(SymbolSourceContext.CLASS);
        
        List<SymbolUnitNode> methods = Arrays
            .stream(clazz.getDeclaredMethods())
            .map(this::parseMethod)
            .collect(Collectors.toList());
        
        unit.setNodes(methods);
        return unit;
    }
    
    private SymbolUnitNode parseMethod(Method method) {
        SymbolUnitNode node = new SymbolUnitNode();
        node.setName(method.getName());
        node.setType("method");
        node.setContent(String.format("%s %s(%s)",
            method.getReturnType().getSimpleName(),
            method.getName(),
            Arrays.stream(method.getParameterTypes())
                .map(Class::getSimpleName)
                .collect(Collectors.joining(", "))));
        
        return node;
    }
}
```

## Usage

### Extract Symbols
```java
SymbolExtractor extractor = new JavaSymbolExtractor();
SymbolUnit symbols = extractor
    .extractFromJavaSource(new File("Engine.java"));
```

### Serialize to JSON
```java
ObjectMapper mapper = new ObjectMapper();
String json = mapper.writeValueAsString(symbols);

// Store in cloud storage
accessor.putObject("symbols/engine", 
                   new ByteArrayInputStream(json.getBytes()));
```

### Deserialize from JSON
```java
SymbolUnit symbols = mapper.readValue(jsonString, SymbolUnit.class);

// Query and display symbol hierarchy
for (SymbolUnitNode node : symbols.getNodes()) {
    printNode(node, 0);
}
```

## Configuration

### Source Types

```java
new SymbolUnit()
    .setSourceContext(SymbolSourceContext.EXECUTABLE_SYM)  // Functions
    .setSourceContext(SymbolSourceContext.METHOD_DECL)     // Methods
    .setSourceContext(SymbolSourceContext.CLASS)           // Classes
    .setSourceContext(SymbolSourceContext.ANNOTATION)      // Decorators
```

### Metadata

```java
unit.setMetadata(Map.of(
    "file", "Engine.java",
    "package", "com.log10x",
    "language", "java",
    "version", "1.0",
    "compiler", "javac 11.0.2"
));
```

## Extension

### Custom Source Implementations

Implement your own scanner for specific code formats:

```java
public interface SymbolExtractor {
    SymbolUnit extract(Object source);
    String serialize(SymbolUnit unit);
    SymbolUnit deserialize(String json);
}

public class PythonSymbolExtractor implements SymbolExtractor {
    // Extract symbols from Python source
}

public class GoSymbolExtractor implements SymbolExtractor {
    // Extract symbols from Go source
}
```

### Custom Context Types

Extend `SymbolSourceContext` enum for domain-specific symbols:

```java
public enum CustomSymbolContext {
    SQL_QUERY,
    CONFIGURATION,
    BUILD_RULE,
    DEPLOYMENT_MANIFEST
}
```

## Integration

### Store in Index
```java
// Extract symbols
SymbolUnit symbols = extractor.extract(sourceCode);

// Write to index
IndexFilterWriter writer = new IndexFilterWriter(accessor, opts);
writer.write(toInputStream(symbols), "symbols-key");
```

### Query Enrichment
```java
// Get query results
IndexQueryReader reader = new IndexQueryReader(accessor, opts);

// Load symbol context
SymbolUnit symbols = loadSymbols(resultKey);

// Enrich results with source information
enrichResults(reader, symbols);
```

## Testing

### Unit Tests
- SymbolUnit serialization/deserialization
- Node hierarchy validation
- Context type mapping

### Integration Tests
- Extract from real source files
- Round-trip JSON serialization
- Query result enrichment

### Performance Tests
- Extraction throughput
- Serialization speed
- Query latency with symbols

## Dependencies

- Jackson for JSON
- Java Parser (for Java extraction)
- Optional: language-specific parsers

## Related

- [Main README](../README.md) — Cloud extensions overview
- [Index Module](../index/README.md) — Symbol storage
- [Compile Pipeline](../compile/README.md) — Scanner integration

---

Extensible framework for extracting and indexing source code symbols.
