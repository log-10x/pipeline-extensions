# Query Module — Query Execution Engine

Complete query execution pipeline for searching indexed log data in cloud storage with filter evaluation, term matching, and result streaming.

## Overview

Transforms query requests into executed searches over cloud-stored objects:

- **Filter Evaluation** — Query filter expression evaluation
- **Term Matching** — Multi-term and single-term filtering
- **Stream Processing** — Efficient streaming of matching results
- **Composite Reading** — Multiple readers as unified source

## Architecture

### Query Execution Flow

```
Query Request
    ↓
IndexQueryReader (composite)
    ├─ IndexQueryWriter (write results)
    ├─ CompositeIndexReader (compose results)
    ├─ QueryFilterEvaluator (eval filters)
    └─ TermFilter (single/multi term)
        ↓
    ObjectStorageIndexAccessor
        ↓
    Cloud Storage (S3, Local, etc.)
```

## Key Classes

### IndexQueryReader
**Main query execution** entry point

```java
IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions()
        .withTerms(Arrays.asList("ERROR"))
        .withStartTime(startMs)
        .withEndTime(endMs));

try (Reader results = reader.read()) {
    // Stream results line-by-line
}
```

**Responsibilities:**
- Load templates and bloom filters
- Filter by time range
- Evaluate search terms
- Return composite reader

### IndexQueryWriter
**Writes query results** to storage

```java
IndexQueryWriter writer = new IndexQueryWriter(
    accessor,
    new IndexQueryOptions());

writer.write(queryResults, "output-key");
```

### QueryFilterEvaluator
**Evaluates filter expressions** during query execution

```java
QueryFilterEvaluator evaluator = new QueryFilterEvaluator();
boolean matches = evaluator.evaluate(expression, event);
```

**Supported Operators:**
- Boolean AND, OR, NOT
- Comparison: ==, !=, <, >, <=, >=
- Field access: event.field
- String matching: contains, startsWith, endsWith

### CompositeIndexReader
**Composes multiple readers** into unified reader

```java
CompositeIndexReader composite = new CompositeIndexReader(
    Arrays.asList(reader1, reader2, reader3));

try (Reader unified = composite.read()) {
    // Reads from all readers in sequence
}
```

### Term Filtering

#### SingleTermFilterReader
Filters for single term matches:

```java
SingleTermFilterReader filter = new SingleTermFilterReader(
    input, "ERROR");
```

#### MultiTermFilterReader
Filters for any matching term from list:

```java
MultiTermFilterReader filter = new MultiTermFilterReader(
    input, Arrays.asList("ERROR", "WARN", "FATAL"));
```

## Usage Patterns

### Basic Query
```java
ObjectStorageIndexAccessor accessor = ...;

IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions()
        .withTerms(Arrays.asList("ERROR")));

try (Reader results = reader.read()) {
    BufferedReader br = new BufferedReader(results);
    String line;
    while ((line = br.readLine()) != null) {
        processLine(line);
    }
}
```

### Time-Range Query
```java
long now = System.currentTimeMillis();
long oneHourAgo = now - 3600000;

IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions()
        .withStartTime(oneHourAgo)
        .withEndTime(now)
        .withTerms(Arrays.asList("ERROR")));
```

### Multiple Term Query
```java
IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions()
        .withTerms(Arrays.asList("ERROR", "TIMEOUT", "FATAL")));
// Matches any of the three terms
```

### Stored Query Results
```java
IndexQueryWriter writer = new IndexQueryWriter(accessor, options);
writer.write(queryResults, "query-results-key");

// Later retrieve results
IndexQueryReader reader = new IndexQueryReader(
    accessor,
    new IndexQueryOptions().withResultsKey("query-results-key"));
```

## Performance Characteristics

### Query Latency
- **Bloom filter checks** — O(1) per object (nanoseconds)
- **Template lookups** — O(1) per object
- **Term matching** — O(n) where n = lines in object
- **Total** — 10-100ms for typical queries (network dependent)

### Throughput
- **Objects checked** — 1000+ per second
- **Lines processed** — 100k+ per second
- **Results streamed** — Limited by output bandwidth

### Memory
- **Filter** — 10-20 bits per indexed element
- **Template** — Varies by content (typically 1-10% of original)
- **Results stream** — Constant regardless of result size

## Configuration

### Query Options
```java
new IndexQueryOptions()
    .withTerms(Arrays.asList("ERROR"))
    .withStartTime(System.currentTimeMillis() - 3600000)
    .withEndTime(System.currentTimeMillis())
    .withBloomFilterAccuracy(0.01)
    .withCacheTemplates(true)
    .withMaxResults(10000)
```

### Filter Options
```java
new InputTermFilterReader(
    stream,
    InputTermFilterReader.SINGLE_TERM,
    "search_term");
```

## Optimization Tips

### Reduce Time Window
Smaller time windows = fewer objects to scan:
```java
.withStartTime(now - 600000)    // 10 minutes (better)
.withEndTime(now)
// Instead of:
.withStartTime(now - 86400000)  // 24 hours (slower)
```

### Cache Templates
Reuse templates for repeated queries:
```java
.withCacheTemplates(true)  // Improves latency for similar queries
```

### Adjust Filter Accuracy
Balance memory vs false positive rate:
```java
.withBloomFilterAccuracy(0.001)  // Aggressive pruning
// vs
.withBloomFilterAccuracy(0.10)   // Memory efficient
```

## Extension

### Custom Filter Implementations
```java
public class RegexTermFilterReader extends InputTermFilterReader {
    private final Pattern pattern;
    
    public RegexTermFilterReader(Reader input, String regex) {
        super(input);
        this.pattern = Pattern.compile(regex);
    }
    
    @Override
    protected boolean matches(String line) {
        return pattern.matcher(line).find();
    }
}
```

### Custom Query Types
```java
public class GeoQueryReader extends BaseIndexReader {
    // Query by geographic location
    // Parse coordinates from events
    // Filter by distance
}
```

## Testing

### Unit Tests
- Filter evaluation logic
- Term matching accuracy
- Composite reader sequencing
- Option validation

### Integration Tests
- End-to-end query execution
- Time range filtering
- Multi-term queries
- Result writer and reader

### Performance Tests
- Query latency under load
- Memory usage profiling
- Throughput benchmarks
- Cache effectiveness

## Related

- [Filter Module](../filter/README.md) — Bloom filters for pruning
- [Access Layer](../access/README.md) — Storage backends
- [Utilities](../util/README.md) — Stream and buffer utilities

## Completion Signalling — `_DONE.json` (R21)

The top-level coordinator writes an atomic JSON marker the instant it
finishes dispatching sub-queries, so programmatic consumers (log10x-mcp,
batch callers) get a **sub-second deterministic completion signal**
instead of polling R18 or running a stability heuristic on the
byte-count markers.

**Where:**

```
{indexObjectPath(queryResults, target)}/{queryId}/_DONE.json
```

On the demo-env bucket, this resolves to:

```
s3://{bucket}/{indexSubpath}/tenx/{target}/qr/{queryId}/_DONE.json
```

**Body (all fields UTF-8 JSON, stable schema):**

```json
{
  "queryId": "...",
  "completedAt": 1776700541264,
  "elapsedMs": 14,
  "reason": "success|empty-range|bloom-miss|match-no-dispatch|unknown|aborted",
  "scanned": 94,
  "matched": 1,
  "skippedSearch": 92,
  "skippedTemplate": 0,
  "streamRequests": 1,
  "streamBlobs": 1,
  "submittedTasks": 12,
  "expectedMarkers": 1
}
```

**When written:** only by the top-level coordinator invocation
(identified by `options.queryScanFunctionParallelTimeslice > 0`).
Recursive local scan sub-queries that share the same qid correctly do
**not** write the marker.

**Consumer contract:**

1. Submit query with `writeResults: true`.
2. Poll `HEAD s3://.../_DONE.json`. S3 strong read-after-write means the
   GET succeeds the instant the coordinator's `putObject` returns
   (measured 1.05s lag on demo-env, 2026-04-20).
3. Read the body. `expectedMarkers` tells you exactly how many
   byte-count markers (workers) to wait for before considering results
   fully written.
4. If `writeResults: true`, poll
   `{indexObjectPath(query,target)}/{queryId}/` until object count ≥
   `expectedMarkers`. Only then read `qr/{queryId}/*.jsonl` payloads.

**Fallback for legacy streamers (pre-R21):** if `_DONE.json` never
appears within a budget, fall back to the pre-R21 byte-count-marker
stability heuristic (two consecutive polls returning the same set).
log10x-mcp's `runStreamerQuery` already implements this fallback.

**Best-effort write:** the coordinator swallows any `putObject` failure
and logs a warn so a transient S3 error does not abort the coordinator's
close path.

---

Enterprise-grade query execution for cloud-stored log data.
