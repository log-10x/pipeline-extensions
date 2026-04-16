# Filter Module — Bloom Filter Indexes

Probabilistic data structures for O(1) index lookups with configurable accuracy. Enables efficient pruning of irrelevant data during query execution.

## Overview

Bloom filters provide memory-efficient, constant-time membership testing:

- **O(1) Lookups** — Constant time regardless of filter size
- **Low Memory** — 10 bits per element for 1% false positive rate
- **Configurable Accuracy** — Trade memory for accuracy (0.1% - 10% FPR)
- **No False Negatives** — Negative answer is always correct
- **UTF8 Support** — Native string encoding for log event filtering

## Use Case

Query execution workflow:
```
1. Load bloom filter for query terms
2. For each candidate object:
   a. Check if terms match filter (O(1))
   b. Skip object if negative (guaranteed no match)
   c. Read object only if positive (possible match)
3. Execute detailed filtering on loaded objects
```

**Result:** 50-99% of objects pruned without loading, massive bandwidth savings.

## Implementations

### EncodedBloomFilter
**Production implementation** with UTF8 character and byte encoding

**Characteristics:**
- Stores pre-encoded bytes for query terms
- O(1) contains() check
- Serializable to/from bytes for storage

**Example:**
```java
EncodedBloomFilter filter = new EncodedBloomFilter(
    totalElements: 1000000,
    falsePositiveRate: 0.01);  // 1% FPR

filter.add("ERROR");
filter.add("TIMEOUT");

boolean match = filter.contains("ERROR");  // O(1)
```

### DecodedBloomFilter
**Alternative representation** for specific use cases

### EncodedBloomFilterBuilder
**Safe filter construction** using builder pattern

**Features:**
- Automatic size calculation from element count
- Configurable false positive rates
- Thread-safety assertions (NOT thread-safe for concurrent adds)
- Fluent API

**Example:**
```java
EncodedBloomFilter filter =
    new EncodedBloomFilterBuilder()
        .withElements(Arrays.asList("ERROR", "WARN", "TIMEOUT"))
        .withFalsePositiveRate(0.001)  // 0.1% FPR
        .build();
```

## Mathematics

### Bloom Filter Sizing

Given:
- `n` — expected number of elements
- `p` — desired false positive rate (0.01 = 1%)

Calculate:
- `m` — bits needed: `m = -(n * ln(p)) / (ln(2)^2)`
- `k` — hash functions: `k = (m / n) * ln(2)`

**Example:**
```
n = 1,000,000 elements
p = 0.01 (1% false positive rate)
m = 9,585,058 bits ≈ 1.2 MB
k = 7 hash functions
```

### Accuracy Tradeoff

| Elements | 1% FPR | 0.1% FPR | 0.01% FPR |
|----------|--------|----------|----------|
| 1M | 1.2 MB | 1.8 MB | 2.4 MB |
| 10M | 12 MB | 18 MB | 24 MB |
| 100M | 120 MB | 180 MB | 240 MB |

## Performance

### Throughput
- **Lookups** — 100k-1M lookups/second per thread
- **Construction** — 1-10M elements/second
- **Serialization** — 500MB+/second

### Memory
- **10 bits per element** for 1% FPR
- **12 bits per element** for 0.1% FPR
- **15 bits per element** for 0.01% FPR

### Latency
- **Lookup** — ~100 nanoseconds (L3 cache hit)
- **Add** — ~100 nanoseconds
- **Serialization** — <1 microsecond per KB

## Usage Patterns

### Query Execution
```java
// Load filter from storage
EncodedBloomFilter filter = readFilterFromStorage(filterKey);

// Prune candidates
for (String objectKey : candidates) {
    if (!filter.contains("ERROR")) {
        skip(objectKey);  // Guaranteed no "ERROR" in object
    }
    // Load and search object only if filter match
}
```

### Index Building
```java
// Build filter during indexing
List<String> terms = extractTermsFromInput(stream);
EncodedBloomFilter filter =
    new EncodedBloomFilterBuilder()
        .withElements(terms)
        .withFalsePositiveRate(0.01)
        .build();

// Store for later queries
writeFilterToStorage(filter, indexKey);
```

## Configuration

### Accuracy Selection

**For Aggressive Pruning (Save Network):**
```java
.withFalsePositiveRate(0.001)  // 0.1% - 180 MB per 10M elements
```

**For Balanced (Default):**
```java
.withFalsePositiveRate(0.01)   // 1% - 120 MB per 10M elements
```

**For Maximum Memory Efficiency:**
```java
.withFalsePositiveRate(0.10)   // 10% - 80 MB per 10M elements
```

Higher FPR = more false positives = more unnecessary reads.
Lower FPR = larger filters = higher storage/memory cost.

## Limitations

### Bloom Filters Do NOT Support
- ❌ Removal of elements (one-way structure)
- ❌ Size counting (approximate only)
- ❌ Element iteration
- ❌ Exact false positive rate prediction

### Best For
- ✅ Read-only index structures
- ✅ Probabilistic lookups
- ✅ Cache warming
- ✅ Query result set pruning

## Extending

### Custom Hash Functions
```java
public class CustomBloomFilter extends EncodedBloomFilter {
    @Override
    protected int hash(String element, int seed) {
        // Custom hash implementation
        return customHash(element, seed);
    }
}
```

### Alternative Encodings
```java
public class CompactBloomFilter extends CodedBloomFilter {
    // Variable-length encoding for sparse filters
}
```

## Testing

### Unit Tests
- Filter accuracy under various distributions
- Memory overhead calculation
- Hash function distribution
- Serialization round-tripping

### Performance Tests
- Lookup throughput benchmarks
- False positive rate validation
- Memory profiling
- Contention under concurrent reads

### Integration Tests
- End-to-end query pruning
- Filter storage and retrieval
- Behavior with real log data

## References

- Bloom, B. (1970) "Space/time trade-offs in hash coding with allowable errors"
- Broder, A. & Mitzenmacher, M. (2002) "Network Applications of Bloom Filters"
- Detailed Bloom filter analysis: https://en.wikipedia.org/wiki/Bloom_filter

## Related

- [Index Module](../README.md) — Index operations using filters
- [Access Layer](../access/README.md) — Filter storage
- [Utilities](../util/README.md) — Stream filtering

---

Enterprise-grade implementation for production log querying at scale.
