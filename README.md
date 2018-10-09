# gometastore

Tools for accessing Hive Metastore over thrift API.

The repository provides a set of tools for accessing Hive Metastore (HMS) directly using
its Thrift interface rather then going via beeline. It can be use to explore and troubleshoot
HMS and to develop other scripts and tools that need to access it.

## Kerberos support

**None of these tools work in kerberos-enabled cluster.**

You should consider using [Java-based toolkit](https://github.com/akolb1/hclient)
 if you need support for Kerberos.

## Installation

Make sure that you have an up-to-date GO environment. Currently `Go 1.11` or higher is required.

On MacOS this can be as easy as running

    brew install go

Building:

    go get -u github.com/akolb1/gometastore/...
    
This command will install tools in `~/go/bin` directory.

* [hmstool][] - CLI for HMS client

[hmstool]: https://github.com/akolb1/gometastore/tree/master/hmstool

* [GO CLient library][]

[GO CLient library]: https://github.com/akolb1/gometastore/tree/master/hmsclient

* [REST front-end for Hive Metastore][]

[REST front-end for Hive Metastore]: https://github.com/akolb1/gometastore/tree/master/hmsweb

* [HMS Benchmarks][]

[HMS Benchmarks]: https://github.com/akolb1/gometastore/tree/master/hmsbench

 
