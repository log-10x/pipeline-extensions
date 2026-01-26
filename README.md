# Log10x Pipeline Extensions

Java extensions for the Log10x engine, enabling custom inputs, outputs, and processors.

## Structure

```
pipeline-extensions/
├── api-extensions/      # Core API interfaces and utilities
├── edge-extensions/     # Edge flavor extensions (outputs, metrics)
└── cloud-extensions/    # Cloud flavor extensions (S3, SQS, Camel)
```

## Extensions

### API Extensions
Core utilities and interfaces used by other extension modules:
- Pipeline factory and endpoint accessors
- HTTP and file utilities
- Pipeline launch request handling

### Edge Extensions
Extensions for edge deployments:
- Log4j2 appender outputs
- Micrometer metrics outputs
- gRPC and Netty support

### Cloud Extensions
Extensions for cloud deployments:
- Apache Camel inputs for log analyzers
- AWS S3 object storage access
- AWS SQS queue integration
- AWS Lambda support

## Building

```bash
./gradlew build
```

Extensions compile to JAR files that are loaded by the Log10x engine at runtime.

## Documentation

- [Extension Development](https://doc.log10x.com/api/)
- [Custom Outputs](https://doc.log10x.com/run/output/)
- [Custom Inputs](https://doc.log10x.com/run/input/)

## License

This repository is licensed under the [Apache License 2.0](LICENSE).

### Fork-Friendly, License Required to Run

This repository is designed for you to fork and extend. You are free to:

- Fork this repository for your organization
- Write custom extensions for your specific needs
- Contribute improvements back to the community

**However, running extensions with Log10x requires a commercial license.**

| What's Open Source | What Requires License |
|-------------------|----------------------|
| Extension source code in this repo | Log10x engine that loads extensions |
| Build scripts and configuration | Log10x apps (Reporter, Optimizer, etc.) |
| Your custom extension code | Executing pipelines with extensions |

Extensions are useless without the Log10x engine. Think of this like writing
plugins for a commercial application - the plugin code is yours, but you need
the application to run it.

**Get a Log10x License:**
- [Pricing](https://log10x.com/pricing)
- [Documentation](https://doc.log10x.com)
- [Contact Sales](mailto:sales@log10x.com)
