# GO Hive Metastore Client

This is the Hive metastore client library for Golang

## Installation

Standard `go get`:

```
$ go get github.com/akolb1/gometastore/hmsbench
```

## Usage & Example

For API usage and examples, see
[![GoDoc](https://godoc.org/github.com/akolb1/gometastore/hmsclient?status.svg)](https://godoc.org/github.com/akolb1/gometastore/hmsclient)


## Example usage:

    import	(
        "log"
        "github.com/akolb1/gometastore/hmsclient"
    )
    
    func printDatabases() {
        client, err := hmsclient.Open("localhost", 9083)
        if err != nil {
            log.Fatal(err)
        }
        defer client.Close()
        databases, err := client.GetAllDatabases()
        if err != nil {
            log.Fatal(err)
        }
        for _, d := range databases {
            fmt.Println(d)
        }
    }
