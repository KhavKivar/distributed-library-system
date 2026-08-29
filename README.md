# Distributed Library System

A distributed file library written in Go and gRPC. Files are split into chunks
and replicated across three data nodes while a coordinator tracks chunk
placement. The system compares two mutual-exclusion strategies for coordinating
concurrent writes.

## Distributed systems concepts

- Centralized mutual exclusion through a coordinator.
- Ricart-Agrawala distributed mutual exclusion.
- Logical clocks and request queues.
- gRPC communication between clients, data nodes, and the name node.
- Chunked upload, distributed placement, and file reconstruction.

## Architecture

```text
Client
  |-- uploads/downloads chunks over gRPC
  |
  +--> DataNode 1 :50051
  +--> DataNode 2 :50052
  +--> DataNode 3 :50053
             |
             +--> NameNode :50055 (metadata and centralized coordination)
```

The three data nodes can coordinate writes through the name node or directly
through Ricart-Agrawala messages, depending on the mode selected by the client.

## Requirements

- Go 1.20 or newer
- Four free local ports: `50051`, `50052`, `50053`, and `50055`

## Run locally

Start each process in a separate terminal, in this order:

```bash
make -C NameNode run
make -C DataNode1 run
make -C DataNode2 run
make -C DataNode3 run
make -C Client run
```

The client asks which coordination mode to use and then offers upload and
download operations. Put your own small test files in `Client/Book/`; repository
history intentionally excludes books and generated runtime chunks.

## Validate

```bash
go build ./...
go test ./...
```

## Repository layout

- `Client/`: interactive upload and download client plus generated gRPC types.
- `DataNode1/`, `DataNode2/`, `DataNode3/`: storage and coordination nodes.
- `NameNode/`: metadata registry and centralized lock coordinator.
