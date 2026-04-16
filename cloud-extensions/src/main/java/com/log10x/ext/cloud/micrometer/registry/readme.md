# Metric Registry Module — Time-Series Output

Factory implementations for publishing metrics to major monitoring backends using Micrometer abstraction layer.

## Overview

Multi-backend metric publishing for cloud-native observability:

- **4 Backends** — CloudWatch, Datadog, Elastic, SignalFx
- **Factory Pattern** — Select backend via configuration
- **Micrometer Abstraction** — Consistent API across backends
- **Type-Safe** — Compile-time verification of metrics

## Supported Backends

### CloudWatch (AWS)
**Native AWS monitoring service**

```java
MetricRegistry registry = 
    new CloudWatchMetricRegistryFactory()
        .createRegistry(new ObjectStorageAccessOptions()
            .withAwsRegion("us-west-2"));

registry.counter("log.events.total").increment();
registry.timer("query.duration").record(duration, TimeUnit.MILLISECONDS);
```

**Features:**
- Native AWS integration
- IAM role support
- Automatic dimension creation
- No additional costs for AWS customers

### Datadog
**SaaS monitoring platform**

```java
MetricRegistry registry = 
    new DataDogMetricRegistryFactory()
        .createRegistry(new ObjectStorageAccessOptions()
            .withApiKey("YOUR_API_KEY"));

registry.gauge("storage.usage", storageBytes);
```

**Features:**
- Real-time dashboards
- Advanced analytics
- Log aggregation integration
- Global SaaS infrastructure

### Elasticsearch
**Time-series database**

```java
MetricRegistry registry = 
    new ElasticMetricRegistryFactory()
        .createRegistry(new ObjectStorageAccessOptions()
            .withEndpoint("https://elasticsearch:9200"));
```

**Features:**
- Open-source option
- On-premises deployment
- Kibana visualization
- Cost-effective for self-hosted

### SignalFx (Splunk)
**Real-time monitoring platform**

```java
MetricRegistry registry = 
    new SignalFxMetricRegistryFactory()
        .createRegistry(new ObjectStorageAccessOptions()
            .withToken("YOUR_TOKEN"));
```

**Features:**
- Real-time alerting
- Advanced correlation analysis
- Integration with Splunk
- High-cardinality metrics

## Architecture

### Factory Pattern

```
MetricRegistryFactory (interface)
    ├─ CloudWatchMetricRegistryFactory
    ├─ DataDogMetricRegistryFactory
    ├─ ElasticMetricRegistryFactory
    └─ SignalFxMetricRegistryFactory
```

**Factory Method:**
```java
public interface MetricRegistryFactory {
    MeterRegistry createRegistry(ObjectStorageAccessOptions opts);
}
```

### Implementation Pattern

```java
public class MyMetricRegistryFactory implements MetricRegistryFactory {
    @Override
    public MeterRegistry createRegistry(ObjectStorageAccessOptions opts) {
        // 1. Extract configuration from options
        String endpoint = opts.getEndpoint();
        String apiKey = opts.getApiKey();
        
        // 2. Create backend-specific config
        MyBackendConfig config = new MyBackendConfig()
            .setEndpoint(endpoint)
            .setApiKey(apiKey);
        
        // 3. Return Micrometer MeterRegistry
        return new MyMeterRegistry(config);
    }
}
```

## Usage Patterns

### Select Backend via Configuration

```java
String backend = System.getenv("METRICS_BACKEND");  // "cloudwatch"

MetricRegistry registry = switch(backend) {
    case "cloudwatch" -> 
        new CloudWatchMetricRegistryFactory().createRegistry(opts);
    case "datadog" -> 
        new DataDogMetricRegistryFactory().createRegistry(opts);
    case "elastic" -> 
        new ElasticMetricRegistryFactory().createRegistry(opts);
    case "signalfx" -> 
        new SignalFxMetricRegistryFactory().createRegistry(opts);
    default -> throw new IllegalArgumentException(backend);
};
```

### Publish Metrics

```java
// Counter
registry.counter("events.processed").increment();
registry.counter("events.errors").increment(errorCount);

// Timer
Timer.Sample sample = Timer.start(registry.timer("query.duration"));
// ... execute query ...
sample.stop();

// Gauge
registry.gauge("active.connections", connections.size());

// Distribution Summary
registry.summary("event.size.bytes")
    .record(eventSizeBytes);
```

### Custom Dimensions/Tags

```java
registry.counter("events.processed",
    "service", "log-processor",
    "environment", "production",
    "region", "us-west-2")
    .increment();
```

## Configuration

### CloudWatch
```properties
metrics.backend=cloudwatch
metrics.cloudwatch.region=us-west-2
metrics.cloudwatch.namespace=Log10x
```

### Datadog
```properties
metrics.backend=datadog
metrics.datadog.apiKey=YOUR_API_KEY
metrics.datadog.site=datadoghq.com
```

### Elasticsearch
```properties
metrics.backend=elastic
metrics.elastic.endpoint=https://elasticsearch:9200
metrics.elastic.username=user
metrics.elastic.password=pass
```

### SignalFx
```properties
metrics.backend=signalfx
metrics.signalfx.token=YOUR_TOKEN
metrics.signalfx.endpoint=https://ingest.us0.signalfx.com
```

## Performance

### Throughput
- **Metric recording** — 100k+ metrics/second
- **Background publishing** — Batched uploads, minimal CPU
- **Network** — Async publishing, non-blocking

### Latency
- **Metric recording** — <100 microseconds
- **Publishing** — Batched every 60 seconds
- **Backend delivery** — 10-60 seconds typical

### Memory
- **Per metric** — ~100 bytes overhead
- **Buffering** — Configurable batch size

## Security

### Credentials Management
- API keys from environment variables
- IAM roles for AWS backends
- OAuth for cloud services
- No credentials in logs or config

### Data Privacy
- HTTPS for all publishing
- Optional encryption for sensitive metrics
- Metric naming patterns allow scrubbing

### Compliance
- SOC2/FedRAMP backends available
- Data residency options
- Audit trail support

## Extension

### Add New Backend

```java
public class MyCustomMetricsFactory implements MetricRegistryFactory {
    @Override
    public MeterRegistry createRegistry(ObjectStorageAccessOptions opts) {
        MyCustomMeterRegistry registry = 
            new MyCustomMeterRegistry();
        
        registry.config()
            .commonTags("service", "log10x")
            .meterFilter(MeterFilter.deny(id -> 
                id.getName().startsWith("jvm")));
        
        return registry;
    }
}
```

### Custom Meter Filters

```java
registry.config().meterFilter(
    MeterFilter.maximumAllowableTags(
        "exception", "type", 100));  // Limit cardinality
```

## Testing

### Unit Tests
- Factory method behavior
- Registry configuration
- Meter creation

### Integration Tests
- Publishing to actual backends
- Authentication validation
- Data verification

### Mocking
```java
@Test
public void testMetrics() {
    MeterRegistry registry = new SimpleMeterRegistry();
    // Test without actual backend
    registry.counter("test.counter").increment();
    assertEquals(1, 
        registry.find("test.counter").counter().count());
}
```

## Monitoring Dashboards

### CloudWatch
- Native AWS dashboards
- Custom dashboard creation
- Alarms and auto-scaling

### Datadog
- Out-of-box dashboards
- Drag-and-drop editing
- Anomaly detection

### Elasticsearch + Kibana
- Open-source dashboards
- Custom queries
- Real-time updates

### SignalFx
- Preconfigured dashboards
- Advanced visualization
- Real-time collaboration

## Performance Optimization

### Reduce Cardinality
```java
registry.config()
    .meterFilter(MeterFilter.deny(id ->
        "tag.value".contains(id.getTag("unuseful"))));
```

### Batch Publishing
```java
registry.config()
    .step(Duration.ofSeconds(10))  // Batch every 10s
    .batchSize(1000);               // Or 1000 metrics
```

### Sample Rates
```java
registry.config()
    .meterFilter(MeterFilter.sample(10));  // Record 10% of metrics
```

## Related

- [Main README](../README.md) — Cloud extensions overview
- [Index Module](../index/README.md) — Indexed queries
- [Camel Integration](../camel/README.md) — Stream processing

---

Enterprise-grade metrics publishing across major monitoring platforms.
