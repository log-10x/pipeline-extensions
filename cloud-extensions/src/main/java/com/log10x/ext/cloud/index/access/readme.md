# Access Layer — Storage Backend Implementations

Abstraction layer providing consistent interface for accessing object storage backends (AWS S3, local filesystem, etc.) with support for byte-range reads, list operations, and authenticated requests.

## Overview

The Access layer implements the `ObjectStorageIndexAccessor` interface, enabling index operations across different storage providers without client code changes.

## Implementations

### AWSIndexAccess
**AWS S3-based storage accessor** for production cloud deployments

**Capabilities:**
- Signed requests (SigV4) for authenticated S3 access
- Byte-range read support for efficient partial file access
- List operations with prefix filtering
- HTTP connection pooling for high throughput
- Configurable region and endpoint for S3-compatible services

**Configuration:**
```java
new AWSIndexAccess(
    new ObjectStorageAccessOptions()
        .withAwsRegion("us-west-2")
        .withBucket("log-storage")
        .withEndpoint("https://s3.us-west-2.amazonaws.com"));
```

**Security:**
- Automatic credential resolution (env vars, instance roles, config files)
- Support for temporary credentials and session tokens
- SigV4 request signing for all operations
- HTTPS-only communication

### LocalIndexAccess
**File system-based accessor** for development, testing, and air-gapped environments

**Capabilities:**
- Direct file system access without network overhead
- Byte-range read support for large files
- Synchronous operations with minimal latency

**Configuration:**
```java
new LocalIndexAccess(
    new ObjectStorageAccessOptions()
        .withLocalPath("/var/log/storage"));
```

**Use Cases:**
- Local development without AWS credentials
- Integration testing with reproducible data
- Air-gapped deployments without cloud connectivity
- Single-node testing environments

## Design

Both implementations extend a common abstract base:

```
ObjectStorageAccessor (interface)
    ↑
    ├── AWSIndexAccess
    ├── LocalIndexAccess
    └── [Custom implementations]
```

### Core Interface Methods

```java
public interface ObjectStorageAccessor {
    // Single object read with byte-range support
    InputStream getObject(String key, long byteStart, long byteEnd);
    
    // Object listing with prefix filter
    List<String> listObjects(String prefix, String continuationToken);
    
    // Object write operations
    void putObject(String key, InputStream data);
}
```

## Performance

### AWSIndexAccess
- **Latency** — 10-100ms per S3 request (network dependent)
- **Throughput** — 100+ concurrent requests via connection pooling
- **Bandwidth** — Limited only by network and S3 request rate (3,500 PUT/s, 5,500 GET/s per prefix)

### LocalIndexAccess
- **Latency** — <1ms per file operation
- **Throughput** — Limited by disk I/O (50-500MB/s depending on hardware)
- **Concurrency** — Unlimited concurrent readers, single writer

## Usage Patterns

### Factory Pattern
Let `IndexAccessorFactory` select the correct implementation:

```java
ObjectStorageIndexAccessor accessor =
    IndexAccessorFactory.createAccessor(options);
```

### Direct Instantiation
Choose explicitly when backend is known:

```java
ObjectStorageIndexAccessor accessor = new AWSIndexAccess(options);
```

## Byte-Range Optimization

Both implementations support efficient partial reads:

```java
// Fetch only bytes 1000-2000 from object
InputStream range = accessor.getObject("index-key", 1000, 2000);
```

Benefits:
- **Network** — 50-99% bandwidth reduction for large objects
- **Latency** — Faster response times from partial reads
- **Cost** — Reduced data transfer charges on cloud providers

## Extension Points

### Add GCS Support
```java
public class GCSIndexAccess implements ObjectStorageAccessor {
    private final Storage storage;
    
    public GCSIndexAccess(ObjectStorageAccessOptions opts) {
        this.storage = StorageOptions.getDefaultInstance().getService();
    }
    
    @Override
    public InputStream getObject(String key, long start, long end) {
        Blob blob = storage.get(bucket, key);
        return blob.downloadTo(
            new ByteRangeOutputStream(start, end));
    }
}
```

### Add Azure Blob Support
```java
public class AzureIndexAccess implements ObjectStorageAccessor {
    private final BlobContainerClient container;
    
    public AzureIndexAccess(ObjectStorageAccessOptions opts) {
        this.container = new BlobContainerClientBuilder()
            .connectionString(opts.getConnectionString())
            .containerName(opts.getContainer())
            .buildClient();
    }
}
```

## Testing Strategy

### Unit Tests
- Mock storage operations
- Verify credential handling
- Test byte-range calculations

### Integration Tests
- Real S3 bucket (testing account)
- Real local filesystem
- Network failure scenarios

### Performance Tests
- Throughput benchmarks per implementation
- Latency percentiles (p50, p95, p99)
- Concurrent request handling

## Security Considerations

### AWS S3
- **Authentication** — IAM roles, temporary credentials, access keys
- **Authorization** — Bucket policies, object ACLs
- **Encryption** — SSE-S3, SSE-KMS, client-side encryption
- **Audit** — CloudTrail logging, VPC endpoints for private access

### Local File System
- **Permissions** — POSIX file ownership and mode bits
- **Encryption** — Filesystem-level (dm-crypt, BitLocker, etc.)
- **Audit** — OS-level file access auditing

## Configuration

### S3 Backends
```properties
storage.provider=aws
storage.aws.region=us-west-2
storage.aws.bucket=log-storage
storage.aws.accessKey=AKIAIOSFODNN7EXAMPLE
storage.aws.secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
storage.aws.endpoint=https://s3.us-west-2.amazonaws.com
```

### Local Backends
```properties
storage.provider=local
storage.local.path=/var/log/storage
```

## Compatibility

- **S3-Compatible** — MinIO, DigitalOcean Spaces, Wasabi, etc.
- **Java Versions** — Java 11+
- **Cloud Providers** — AWS S3, on-premises Kubernetes, edge deployments

---

For index operations using these accessors, see [parent README](../README.md).
