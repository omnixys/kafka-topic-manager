# @omnixys/kafka-topic-manager

Executable Kafka/Redpanda topic reconciler for Omnixys infrastructure.

The package imports `@omnixys/kafka` and treats that package as the only topic
contract. It does not own topic names. It resolves the catalog, validates it,
connects to Redpanda with `rpk`, creates missing topics, reconciles mutable
configuration, reports drift, and exits with a deterministic summary.

## CLI

```bash
omnixys-kafka-topic-manager reconcile --config /config/topic-manager.json
```

Environment override:

```bash
REDPANDA_BROKERS=kafka.omnixys-data.svc.cluster.local:9092
```

The manager expects `rpk` to be present in `PATH`. The supplied Dockerfile copies
`rpk` from the Redpanda image into a Node runtime image.

## Version Line

`@omnixys/kafka-topic-manager` must be released with the same version as
`@omnixys/kafka`. The package build runs `check:version-line` before compiling
and fails if the dependency does not exactly match the manager version.
