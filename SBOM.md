# Software Bill of Materials (SBOM)

**Last updated:** February 2026
**Java version:** 21
**Build system:** Gradle

This document lists the third-party open-source dependencies used by Log10x pipeline components. All dependencies are sourced from Maven Central.

---

## Pipeline Extensions (this repository)

### api-extensions

| Dependency | Version | License |
|---|---|---|
| org.apache.logging.log4j:log4j-core | 2.24.3 | Apache 2.0 |
| io.micrometer:micrometer-registry-prometheus | 1.14.7 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-xml | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.15.2 | Apache 2.0 |
| org.xerial.snappy:snappy-java | 1.1.10.3 | Apache 2.0 |
| com.google.protobuf:protobuf-java | 4.32.0 | BSD 3-Clause |
| com.log10x:prometheus-remote-write | 0.9.0 | Apache 2.0 |

### edge-extensions

| Dependency | Version | License |
|---|---|---|
| org.apache.logging.log4j:log4j-core | 2.24.3 | Apache 2.0 |
| io.micrometer:micrometer-registry-prometheus | 1.14.7 | Apache 2.0 |
| io.prometheus:prometheus-metrics-exporter-httpserver | 1.4.1 | Apache 2.0 |
| io.prometheus:prometheus-metrics-exporter-pushgateway | 1.4.1 | Apache 2.0 |
| com.google.protobuf:protobuf-java | 4.32.0 | BSD 3-Clause |
| org.xerial.snappy:snappy-java | 1.1.10.3 | Apache 2.0 |
| com.fasterxml.jackson.core:jackson-core | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.core:jackson-databind | 2.15.2 | Apache 2.0 |
| io.grpc:grpc-protobuf | 1.76.0 | Apache 2.0 |
| io.grpc:grpc-stub | 1.76.0 | Apache 2.0 |
| io.grpc:grpc-netty | 1.76.0 | Apache 2.0 |
| io.opentelemetry.proto:opentelemetry-proto | 1.8.0-alpha | Apache 2.0 |
| io.netty:netty-transport-classes-epoll | 4.1.124.Final | Apache 2.0 |
| io.netty:netty-transport-classes-kqueue | 4.1.124.Final | Apache 2.0 |
| io.netty:netty-transport-native-unix-common | 4.1.124.Final | Apache 2.0 |
| io.netty:netty-transport-native-epoll | 4.1.124.Final | Apache 2.0 |
| io.netty:netty-transport-native-kqueue | 4.1.124.Final | Apache 2.0 |
| com.log10x:prometheus-remote-write | 0.9.0 | Apache 2.0 |

### cloud-extensions

| Dependency | Version | License |
|---|---|---|
| it.unimi.dsi:fastutil | 8.5.8 | Apache 2.0 |
| org.apache.logging.log4j:log4j-core | 2.24.3 | Apache 2.0 |
| org.apache.logging.log4j:log4j-slf4j2-impl | 2.24.3 | Apache 2.0 |
| org.apache.httpcomponents:httpclient | 4.5.14 | Apache 2.0 |
| commons-io:commons-io | 2.18.0 | Apache 2.0 |
| io.micrometer:micrometer-registry-datadog | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-elastic | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-signalfx | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-cloudwatch2 | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-influx | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-prometheus | 1.14.7 | Apache 2.0 |
| io.prometheus:prometheus-metrics-exporter-httpserver | 1.4.1 | Apache 2.0 |
| org.apache.camel:camel-core | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-stream | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-netty-http | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-netty | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-jackson | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-yaml-dsl | 4.14.0 | Apache 2.0 |
| org.apache.camel:camel-jsonpath | 4.14.0 | Apache 2.0 |
| com.baqend:bloom-filter | 2.2.5 | MIT |
| software.amazon.awssdk:lambda | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:sqs | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:s3 | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:sts | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:auth | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:aws-crt-client | 2.31.63 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-xml | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.15.2 | Apache 2.0 |
| org.unbescape:unbescape | 1.1.6.RELEASE | Apache 2.0 |
| org.kohsuke:github-api | 1.326 | MIT |

---

## Pipeline Core (log-10x/l1x)

### Core libraries

| Dependency | Version | License |
|---|---|---|
| org.apache.logging.log4j:log4j-core | 2.24.3 | Apache 2.0 |
| org.apache.logging.log4j:log4j-layout-template-json | 2.24.3 | Apache 2.0 |
| org.apache.logging.log4j:log4j-slf4j2-impl | 2.24.3 | Apache 2.0 |
| org.apache.logging.log4j:log4j-jcl | 2.24.3 | Apache 2.0 |
| com.fasterxml.jackson.core:jackson-core | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.core:jackson-databind | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.15.2 | Apache 2.0 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-xml | 2.15.2 | Apache 2.0 |
| commons-io:commons-io | 2.18.0 | Apache 2.0 |
| org.apache.commons:commons-compress | 1.27.1 | Apache 2.0 |
| it.unimi.dsi:fastutil | 8.5.8 | Apache 2.0 |
| com.google.protobuf:protobuf-java | 4.32.0 | BSD 3-Clause |
| info.picocli:picocli | 4.6.2 | Apache 2.0 |
| io.micrometer:micrometer-core | 1.14.7 | Apache 2.0 |
| io.micrometer:micrometer-registry-prometheus | 1.14.7 | Apache 2.0 |
| org.unbescape:unbescape | 1.1.6.RELEASE | Apache 2.0 |
| org.kohsuke:github-api | 1.326 | MIT |

### Data processing

| Dependency | Version | License |
|---|---|---|
| com.univocity:univocity-parsers | 2.9.1 | Apache 2.0 |
| com.maxmind.geoip2:geoip2 | 2.15.0 | Apache 2.0 |
| com.jayway.jsonpath:json-path | 2.8.0 | Apache 2.0 |
| com.networknt:json-schema-validator | 1.5.2 | Apache 2.0 |
| org.scijava:parsington | 2.0.0 | BSD 2-Clause |
| org.eclipse.parsson:parsson | 1.1.7 | EPL 2.0 |
| org.msgpack:msgpack-core | 0.9.3 | Apache 2.0 |
| org.msgpack:jackson-dataformat-msgpack | 0.9.3 | Apache 2.0 |

### Compression and hashing

| Dependency | Version | License |
|---|---|---|
| org.xerial.snappy:snappy-java | 1.1.10.3 | Apache 2.0 |
| org.lz4:lz4-java | 1.8.0 | Apache 2.0 |
| net.openhft:zero-allocation-hashing | 0.12 | Apache 2.0 |

### Code generation and parsing

| Dependency | Version | License |
|---|---|---|
| org.antlr:antlr4-runtime | 4.13.2 | BSD 3-Clause |
| com.github.javaparser:javaparser-core | 3.26.4 | LGPL 3.0 / Apache 2.0 |
| org.ow2.asm:asm | 9.7.1 | BSD 3-Clause |
| org.scalameta:scalameta_2.13 | 4.7.8 | BSD 3-Clause |
| org.commonmark:commonmark | 0.22.0 | BSD 2-Clause |
| org.commonmark:commonmark-ext-gfm-tables | 0.22.0 | BSD 2-Clause |
| org.commonmark:commonmark-ext-yaml-front-matter | 0.22.0 | BSD 2-Clause |
| com.github.spullara.mustache.java:compiler | 0.9.14 | Apache 2.0 |

### Fluent data pipeline

| Dependency | Version | License |
|---|---|---|
| org.komamitsu:fluency-fluentd | 2.7.3 | Apache 2.0 |
| org.komamitsu:fluency-fluentd-ext | 2.7.3 | Apache 2.0 |
| io.github.technologize:fluentd-log4j-appender | 1.0.0 | Apache 2.0 |

### Metrics and observability

| Dependency | Version | License |
|---|---|---|
| com.log10x:prometheus-remote-write | 0.9.0 | Apache 2.0 |
| io.github.green4j:green-jelly | 0.1.1 | Apache 2.0 |
| me.tongfei:progressbar | 0.9.5 | MIT |

### AI

| Dependency | Version | License |
|---|---|---|
| dev.langchain4j:langchain4j | 1.7.1 | Apache 2.0 |

### Cloud runtime (Quarkus)

| Dependency | Version | License |
|---|---|---|
| io.quarkus.platform:quarkus-bom | 3.30.2 | Apache 2.0 |
| io.quarkiverse.amazonservices:quarkus-amazon-services-bom | 3.12.1 | Apache 2.0 |
| software.amazon.awssdk:url-connection-client | 2.31.63 | Apache 2.0 |
| software.amazon.awssdk:sts | 2.31.63 | Apache 2.0 |

### Build infrastructure

| Dependency | Version | License |
|---|---|---|
| org.jfrog.artifactory.client:artifactory-java-client-api | 2.11.1 | Apache 2.0 |
| org.jfrog.artifactory.client:artifactory-java-client-httpClient | 2.11.1 | Apache 2.0 |
| org.apache.httpcomponents:httpclient | 4.5.14 | Apache 2.0 |
| org.atteo:evo-inflector | 1.3 | Apache 2.0 |

### Native image (GraalVM)

| Dependency | Version | License |
|---|---|---|
| org.graalvm.buildtools:native-gradle-plugin | 0.10.2 | UPL 1.0 |

---

## Build and runtime environment

| Component | Version |
|---|---|
| Java | 21 |
| Gradle | 9.x |
| GraalVM (native builds) | CE for JDK 21 |
| Docker base image | log10x/pipeline-10x |

---

## License summary

| License | Count |
|---|---|
| Apache 2.0 | ~60 |
| BSD 2-Clause / 3-Clause | ~7 |
| MIT | ~3 |
| EPL 2.0 | 1 |
| UPL 1.0 | 1 |
| LGPL 3.0 (dual-licensed with Apache 2.0) | 1 |

All dependencies are compatible with commercial use. No copyleft-only licenses are used in runtime dependencies.
