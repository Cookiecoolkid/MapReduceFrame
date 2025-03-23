# MapReduceFrame

## Introduction
Based on MIT 6.824 Lab1, this project is a simple implementation of MapReduce framework.

Coordinators and workers communicate with each other through RPC by Unix domain socket. The coordinator assigns tasks to workers and collects the results. Workers execute the tasks and send the results back to the coordinator.

## Usage

- Sequential Mode

```shell
$ cd main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```

- Coordinator/Worker Mode

```shell
$ cd main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
$ go run mrworker.go wc.so
```