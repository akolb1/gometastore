# HmsWEB - HTTP interface to Hive Metastore

## Installation

`go get github.com/akolb1/gometastore/hmsweb`

## Starting web server

```bash
$ hmsweb -h
Usage of hmsweb:
  -hmsport int
        HMS Thrift port (default 9083)
  -port int
        web service port (default 8080)
$ hmsweb
```

## Examples

Examples below use [httpie][]

[httpie]: https://httpie.org

### Listing Databases

`$ http --body localhost:8080/hms.host.org/databases`

```json
[
    "localhost:8080/hms.host.org/databases/default",
    "localhost:8080/hms.host.org/databases/foo",
    "localhost:8080/hms.host.org/databases/foobar"
]
```

The `/databases` part is optional, so the following is the same:

`http --body localhost:8080/hms.host.org`

#### Compact listing

`$ http --body localhost:8080/hms.host.org/databases Compact==true`
```json
[
    "default",
    "foo",
    "foobar"
]
```

This is equivalent to the following `HTTP GET` request:

```$xslt
$ curl 'localhost:8080/hms.host.org/databases?Compact=true'
```

### Getting database details

`$ http --body localhost:8080/hms.host.org/databases/default`
`$ http --body localhost:8080/hms.host.org/default`

```json
{
    "description": "Default Hive database",
    "location": "hdfs://localhost:8020/user/hive/warehouse",
    "name": "default",
    "owner": "public"
}
```

### Listing tables in a database

`$ http --body localhost:8080/hms.host.org/default/`

```json
[
    "localhost:8080/hms.host.org/databases/default/customers",
    "localhost:8080/hms.host.org/databases/default/sample_07",
    "localhost:8080/hms.host.org/databases/default/sample_08",
    "localhost:8080/hms.host.org/databases/default/web_logs"
]
```

As with databases, `Compact=true` parameter can be used to display just table names:

`$ http --body localhost:8080/hms.host.org/default/ Compact==true`

```json
[
    "customers",
    "sample_07",
    "sample_08",
    "web_logs"
]
```

### Getting table details

`$ http --body localhost:8080/hms.host.org/default/customers`

```json
{
    "dbName": "default",
    "owner": "admin",
    "parameters": {
        "COLUMN_STATS_ACCURATE": "true"
    },
    "partitionKeys": [],
    "sd": {
        "bucketCols": [],
        "cols": [
            {
                "comment": "",
                "name": "id",
                "type": "int"
            }
        ],
        "inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "location": "hdfs://hms.host.org:8020/user/hive/warehouse/customers",
        "outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "parameters": {},
        "serdeInfo": {
            "name": "",
            "parameters": {
                "serialization.format": "1"
            },
            "serializationLib": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        }
    }
    "tableName": "customers",
    "tableType": "MANAGED_TABLE",
}
```

### Listing partitions within a table

`$ http --body localhost:8080/hms.host.org/default/web_logs/`

```json
[
    "localhost:8080/hms.host.org/default/web_logs/date=2015-11-18",
    "localhost:8080/hms.host.org/default/web_logs/date=2015-11-19",
    "localhost:8080/hms.host.org/default/web_logs/date=2015-11-20",
    "localhost:8080/hms.host.org/default/web_logs/date=2015-11-21"
]
```

### Listing information about specific partition

`$ http --body localhost:8080/hms.host.org/default/web_logs/date=2015-11-18`

```json
{
    "dbName": "default",
    "lastAccessTime": 0,
    "parameters": {
        "COLUMN_STATS_ACCURATE": "false",
    },
    "sd": {
        "bucketCols": [],
        "cols": [
            {
                "comment": "",
                "name": "_version_",
                "type": "bigint"
            },
            {
                "comment": "",
                "name": "id",
                "type": "string"
            }
        ],
        "compressed": false,
        "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "location": "hdfs://hms.host.org:8020/user/admin/2015_11_18",
        "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "parameters": {},
        "serdeInfo": {
            "name": "",
            "parameters": {
                "field.delim": ",",
                "serialization.format": ","
            },
            "serializationLib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        },
        "storedAsSubDirectories": false
    },
    "tableName": "web_logs",
    "values": [
        "2015-11-18"
    ]
}
```