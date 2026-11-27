# dfe-transform-vrl architecture

dfe-transform-vrl runs your VRL programs over a DFE source's records, between its land topic and dfe-loader. The VRL engine is compiled into the binary, so there is no Vector process and the memory it uses is bounded.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    nodeBorder: "#334155"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L3 Components -- dfe-transform-vrl
    accDescr: Records arrive from the stage in front, a Kafka land topic or a gRPC push. Inside dfe-transform-vrl the scalo batch engine hands each block to the process closure, which runs the program the VRL compiler built on scalo's worker pool. The produce sink sends the out-batch to the next stage, a Kafka load topic or a gRPC listener.

    subgraph IN["Stage in front"]
        direction LR
        upstream("<b>Source</b><br/><span>*_land topic or gRPC push</span>")
    end

    subgraph APP["dfe-transform-vrl"]
        direction LR
        engine("<b>Batch engine</b><br/><span>scalo, owns the loop</span>")
        compiler("<b>VRL compiler</b><br/><span>once, at startup</span>")
        pool("<b>Worker pool</b><br/><span>scalo threads</span>")
        process("<b>Process closure</b><br/><span>parse, run VRL, serialise</span>")
        produce("<b>Produce sink</b><br/><span>Kafka or gRPC</span>")
        engine --> process
        compiler -.-> process
        pool -.-> process
        process --> produce
    end

    subgraph OUT["Next stage"]
        direction LR
        downstream("<b>Sink</b><br/><span>*_load topic or gRPC</span>")
    end

    IN -->|records| APP
    APP -->|out-batch| OUT
  class upstream,downstream bus
  class engine,compiler,pool,process,produce part
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style IN fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style APP fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style OUT fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L3 Components -- dfe-transform-vrl. The arrows from VRL compiler and Worker pool bring what the closure runs with, not records.* Up: [L2 Services -- DFE deployment](https://github.com/hyperi-io/dfe-infra/blob/main/docs/architecture.md). Down: [L4 Detail -- Process closure](internals.md).

## How data flows

1. The **Batch engine** receives a block of records. On the bus it is a governed Kafka consumer on `{source}_land`. On the direct transport it is a Push listener on port 6000.
2. The **VRL compiler** built one program at startup from every `.vrl` file, in filename order.
3. The **Process closure** parses each record's JSON, runs the program on the **Worker pool** in parallel and serialises the survivors.
4. The **Produce sink** sends the whole out-batch to `{source}_load`, or by gRPC to the next stage's listener.
5. Once the send returns `Ok`, the batch engine releases the block: Kafka commits the offsets, or the Push listener answers its sender.

Source and sink transports are chosen apart, so Kafka in and gRPC out is valid, and the reverse.

## Components

| Part | Role | Page |
|---|---|---|
| Batch engine | scalo's `BatchEngine::pipeline`: receive, process, send, release, with self-regulation on intake | scalo-rs |
| VRL compiler | Compiles the transform files into one program, after checking the memory floor | [internals.md](internals.md#how-it-works) |
| Worker pool | scalo's thread pool the closure runs records on | scalo-rs |
| Process closure | Parses JSON, runs the program, serialises the result | [internals.md](internals.md) |
| Produce sink | Sends the out-batch to Kafka or gRPC | [internals.md](internals.md#interfaces) |

This crate supplies the compiler, the closure, the sink, the config and the enrichment tables. scalo runs the loop.

## External dependencies

| Dependency | Needed for | Swappable |
|---|---|---|
| Kafka | The bus transport, in and out | Any broker `source.brokers` and `sink.brokers` reach. The direct transport needs none |
| A DFE service over gRPC | The direct transport's sink, `http://dfe-loader:6000` by default | Any stage that runs a Push listener |
| Dead-letter queue | Records the sink refuses for good | Kafka or a file directory (`dlq.file.path`) |
| Enrichment table files | `get_enrichment_table_record` and `find_enrichment_table_records` | CSV, JSON, YAML, STIX, MMDB and SQLite, mounted as a file set |

## Deployment variants

| Variant | What runs |
|---|---|
| Kubernetes `single`, `scale`, Docker `single` | On by default. dfe-engine runs one instance per source that binds a VRL transform, and removes it once no source does |
| Kubernetes `slim`, `mesh` | Not deployed by default |

The released chart scales on CPU. Consumer-group lag rises when a downstream stage breaks, and more pods can't fix that, so lag is not a trigger. dfe-infra's values add the scaling-pressure trigger in `single`, `scale` and `mesh`. `slim` scales on CPU alone.

The Push port opens when `config.source.transport` is `direct` or `grpc`. A program change is a change to the transforms file set, and the chart rolls the pod.

## Design considerations

**Security.** The host, network, file and decompression functions (`get_env_var`, `get_hostname`, `http_request`, `dns_lookup`, `reverse_dns`, `parse_proto`, `encode_proto`, `validate_json_schema`, `decode_gzip`, `decode_zlib`, `decode_zstd`, `decode_lz4`, `decode_snappy`) don't compile, and nor do the file arguments `parse_etld`'s `psl` and `parse_groks`' `alias_sources`. A transform can't read the host's environment or files, reach the network, or inflate a record without a bound.

**Reliability.** Delivery is at least once, per batch. A crash between send and release delivers the whole block again, so VRL programs must be idempotent. A record VRL drops still releases its offset, so a filtering transform never stalls its source. Self-regulation brakes intake alone. Braking the sink too would deadlock the loop.

**Operations.** Health and metrics answer on 9090. A transform with no topics or no program starts Ready and idle, with `pipeline_idle` at 1, so it can be deployed before its program exists. A transport reports healthy before it reaches a broker, so a pod that never reached one still reports Ready.

**Performance.** JSON parses with SIMD in sonic-rs. The consumer prefetch and producer queue are bounded by `source.max_buffer_bytes` and `sink.max_buffer_bytes`. Compilation scales with the program, and below the memory floor the start is refused with the numbers, not killed mid-compile.

## Why it's built this way

**VRL in process, not Vector.** Vector has no memory cap, so a pod limit has to cover the worst case Vector might reach. Most pipelines use VRL alone. A pipeline that needs `lua`, `aggregate`, `dedupe`, `throttle` or `sample` uses [dfe-transform-vector](https://github.com/hyperi-io/dfe-transform-vector), which keeps the subprocess and pays for it.

| Aspect | dfe-transform-vrl | dfe-transform-vector |
|--------|-------------------|---------------------|
| Transform engine | VRL crate (in-process) | Vector subprocess |
| Kafka source/sink | scalo's `KafkaTransport` over rdkafka (wrapper-controlled) | Vector-managed |
| Memory control | Bounded consumer and producer buffers, scalo's memory guard | None (Vector unbounded) |
| Supported transforms | VRL | All Vector transforms |
| Container image size | Smaller (Rust binary) | Larger (Rust + Vector) |
| Process model | Single process | Wrapper + subprocess |
| Startup time | Fast (compile VRL) | Slower (spawn + healthcheck Vector) |
| Crash recovery | Restart binary | Restart subprocess + binary |

**Compile once.** The program is immutable for the process lifetime. Tables and functions are fixed when it compiles, and a roll is the one way to change either.

**scalo owns the loop.** The batch engine, governor and Kafka transport are the same in every DFE Rust service, so this crate holds the transform and nothing else.

**JSON as the one format.** MessagePack was dropped in DFE 2.2. A record that isn't JSON is dropped and counted, never redelivered.

## Diagram map

- [Figure 1. L3 Components -- dfe-transform-vrl](architecture.md)
  - [L4 Detail -- Process closure](internals.md)

## What's next

- [config.example.yaml](../config.example.yaml), the annotated config, and [config-schema.yaml](config-schema.yaml), every key
- [reference/configuration.md](reference/configuration.md), env overrides, Kafka TLS, what reloads and the chart
- [pipelines/filebeat/README.md](../pipelines/filebeat/README.md), the legacy filebeat bundle
- [CONTRIBUTING.md Context](../CONTRIBUTING.md#context), where things live and the commands that test a change
