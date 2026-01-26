# Camel Module — Stream Processing & Integration

Apache Camel-based input processors for cloud analytics streams with authentication, signing, and custom output stream handling.

## Overview

Extends Log10x pipeline with Apache Camel integration for:

- **Stream Routing** — Event stream processing and transformation
- **Cloud Authentication** — AWS signing, HTTP authentication
- **Custom Outputs** — Stream-based output processors
- **Security** — Flexible SSL/TLS configuration

## Architecture

### Component Stack

```
EventCamelStreamRouteBuilder (route definition)
    ├─ BaseRouteProcessor (abstract base)
    │   ├─ CamelStreamProcessor<T> (generic template)
    │   ├─ AwsSignatureProcessor (AWS requests)
    │   ├─ HttpAuthProcessor (HTTP auth)
    │   └─ CamelEvaluatorBean (evaluation)
    └─ Stream Utilities
        ├─ CamelContextInputStream
        ├─ CamelStreamProcessor
        └─ InsecureSslContextParameters
```

## Key Classes

### EventCamelStreamRouteBuilder
**Route definition** for analyzer input streams

```java
EventCamelStreamRouteBuilder builder =
    new EventCamelStreamRouteBuilder();

// Defines routes:
// from("analyzer-input")
//   .process(new AwsSignatureProcessor())
//   .to("s3://bucket/events");
```

**Responsibilities:**
- Define input/output endpoints
- Configure route processors
- Handle error routing
- Manage stream lifecycle

### BaseRouteProcessor
**Abstract base** for all route processors

```java
public abstract class BaseRouteProcessor implements Processor {
    protected String getProperty(Exchange exchange, 
                                 String name, 
                                 String defaultValue) { }
    
    protected Integer getIntProperty(Exchange exchange, 
                                     String name) { }
    
    protected Object getObjectProperty(Exchange exchange, 
                                       String name) { }
}
```

**Responsibilities:**
- Provide utility methods for property extraction
- Type conversion (String, Integer, Object)
- Fallback to headers/properties
- Consistent error handling

### CamelStreamProcessor<T>
**Generic template** for custom stream output processors

```java
public abstract class CamelStreamProcessor<T>
    extends BaseRouteProcessor {
    
    protected abstract T createOutputStream(Exchange exchange);
    
    @Override
    public void process(Exchange exchange) {
        T stream = createOutputStream(exchange);
        // Replace original stream
        exchange.getIn().setBody(stream);
    }
}
```

**Usage:**
```java
public class S3StreamProcessor extends CamelStreamProcessor<S3OutputStream> {
    @Override
    protected S3OutputStream createOutputStream(Exchange exchange) {
        String bucket = getProperty(exchange, "s3.bucket", "logs");
        String key = getProperty(exchange, "s3.key", 
                                 UUID.randomUUID().toString());
        return new S3OutputStream(bucket, key);
    }
}
```

### AwsSignatureProcessor
**AWS Signature V4** request signing

```java
AwsSignatureProcessor processor = new AwsSignatureProcessor();
// Automatically signs all AWS requests
// Handles credential resolution
// Updates auth headers
```

**Features:**
- Automatic SigV4 signing
- Credential resolution from environment/IAM roles
- Region-aware request signing
- Compatible with all AWS services

### HttpAuthProcessor
**HTTP authentication** setup

```java
HttpAuthProcessor processor = new HttpAuthProcessor()
    .withBasicAuth("username", "password");
// or
    .withBearerToken("token");
```

**Supported Auth Types:**
- Basic Authentication
- Bearer Token (OAuth)
- API Key header injection
- Custom auth headers

### CamelContextInputStream
**Wraps Camel exchange** data as InputStream

```java
InputStream in = new CamelContextInputStream(exchange);
// Read message data from exchange
```

### InsecureSslContextParameters
**Bypass SSL verification** (dev/testing only)

```java
SSLContextParameters insecure = 
    new InsecureSslContextParameters();
// Disables certificate validation
// DO NOT use in production
```

## Usage Patterns

### Basic Route with Authentication

```java
from("analyzer-input")
    .process(new AwsSignatureProcessor())
    .process(new HttpAuthProcessor()
        .withBearerToken(getToken()))
    .to("https://api.example.com/events");
```

### Custom Stream Output

```java
from("analyzer-input")
    .process(new S3StreamProcessor())
    .process(new CompressionProcessor())
    .to("direct:persist");
```

### Error Handling

```java
from("analyzer-input")
    .errorHandler(deadLetterChannel("jms:queue:errors"))
    .process(new AwsSignatureProcessor())
    .onException(IOException.class)
        .maximumRedeliveries(3)
        .redeliveryDelay(1000);
```

## Configuration

### AWS Signing
```properties
camel.aws.region=us-west-2
camel.aws.accessKey=AKIA...
camel.aws.secretKey=...
```

### HTTP Authentication
```properties
camel.http.authType=bearer
camel.http.token=...
```

### Stream Processing
```properties
camel.stream.bufferSize=65536
camel.stream.timeout=30000
```

## Performance

### Throughput
- **Event processing** — 100k+ events/second
- **AWS signing** — Minimal overhead (<1% CPU)
- **HTTP auth** — Constant per request

### Latency
- **Route processing** — <1ms per event
- **Authentication** — 1-5ms overhead
- **Stream creation** — 10-100ms (first time)

## Security Considerations

### AWS Credentials
- Resolved from: environment variables, IAM roles, config files
- Never log credentials
- Use temporary credentials when possible

### HTTP Authentication
- HTTPS only (no plaintext auth)
- Bearer tokens in Authorization header
- Support for token rotation

### SSL/TLS
- Default: Verify certificates
- `InsecureSslContextParameters` only for dev/testing
- Custom truststore support

## Extension

### Custom Processor

```java
public class MyCustomProcessor extends BaseRouteProcessor {
    @Override
    public void process(Exchange exchange) throws Exception {
        String data = exchange.getIn().getBody(String.class);
        String transformed = transform(data);
        exchange.getIn().setBody(transformed);
    }
    
    private String transform(String data) {
        // Custom transformation logic
        return data.toUpperCase();
    }
}
```

### Custom Stream Output

```java
public class GCSStreamProcessor extends CamelStreamProcessor<GCSOutputStream> {
    @Override
    protected GCSOutputStream createOutputStream(Exchange exchange) {
        String bucket = getProperty(exchange, "gcs.bucket", "logs");
        return new GCSOutputStream(bucket);
    }
}
```

## Testing

### Unit Tests
- Property extraction
- Auth header generation
- Stream creation

### Integration Tests
- Full route processing
- AWS signature validation
- HTTP request/response

### Mocking
```java
@Test
public void testRoute() {
    context.createProducerTemplate()
        .sendBody("direct:start", testData);
    
    assertThat(resultQueue).hasSize(1);
}
```

## Dependencies

- Apache Camel 3.x+
- AWS SDK for Java
- Jackson for serialization

## Related

- [Main README](../README.md) — Cloud extensions overview
- [Micrometer](../micrometer/README.md) — Metrics output
- [GitHub Integration](../github/README.md) — GitHub output

---

Enterprise-grade stream processing and cloud integration.
