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

### Creating Database

To create a database, a POST request should be sent using URL ending with the database name and
a JSON body describing the database.

`$ http --body localhost:8080/hms.host.org/databases/mydb owner=hive`
```json
{
    "location": "hdfs://localhost:8020/user/hive/warehouse/mydb.db",
    "name": "mydb",
    "owner": "hive",
}
```

### Dropping Hive database

Dropping a table is performed by sending DELETE request to the database URL.

`$ http DELETE localhost:8080/databases/mydb` 

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

### Creating new Table

Table is created by sending POST request to an URL ending with the new table name.
The message BODY may include the following information:

* owner
* parameters
* columns (list of fieldSchema objects)
* partitions (list of FieldSchena objects)

`$ http localhost:8080/hms.host.org/databases/foo/mytab owner=hive columns:='[{"name": "id"}]' partitions:='[{"name": "date"}]' parameters:='{"p1": "val1"}'`

### Dropping Hive Table

Dropping a table is performed by sending DELETE request to the database URL.

`$ http DELETE localhost:8080/hms.host.org/databases/foo/mytab` 

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

If `Compact` option is true, only partition names will be shown:

`http --body localhost:8080/hms.host.org/databases/default/web_logs/ Compact==t`

```json
[
    "date=2015-11-18",
    "date=2015-11-19",
    "date=2015-11-20",
    "date=2015-11-21"
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

If the caller is only insterested in partition location, specifying `Location=t` changes the output format:

`http --body localhost:8080/hms.host.org/databases/default/web_logs/ Location==t`

```json
{
    "dbName": "default",
    "partitions": [
        {
            "location": "hdfs://hms.host.org:8020/user/admin/2015_11_18",
            "values": [
                "2015-11-18"
            ]
        },
        {
            "location": "hdfs://hms.host.org:8020/user/admin/2015_11_19",
            "values": [
                "2015-11-19"
            ]
        },
        {
            "location": "hdfs://hms.host.org:8020/user/admin/2015_11_20",
            "values": [
                "2015-11-20"
            ]
        },
        {
            "location": "hdfs://hms.host.org:8020/user/admin/2015_11_21",
            "values": [
                "2015-11-21"
            ]
        }
    ],
    "tableName": "web_logs"
}
```

### Adding new partition

Partition is added by sending POST request to the URL `/{host}/databases/{dbName}/{tableName}/`
The message BODY may include the following information:

* values
* parameters (dictionary of name/value strings)
* location - partition location. By default location is constructed from values.

`http --body localhost:8080/hms.host.org/databases/default/mytable/ values:='["d1", "t1"]'`

```json
{
    "createTime": 1514942963,
    "dbName": "default",
    "lastAccessTime": 0,
    "parameters": {
        "transient_lastDdlTime": "1514942963"
    },
    "sd": {
        "bucketCols": [],
        "cols": [
            {
                "comment": "",
                "name": "id",
                "type": "string"
            }
        ],
        "compressed": false,
        "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "location": "hdfs://localhost:8020/user/hive/warehouse/akolb/date=d1/time=t1",
        "numBuckets": 0,
        "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "parameters": {},
        "serdeInfo": {
            "name": "hive",
            "parameters": {},
            "serializationLib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        },
        "skewedInfo": {
            "skewedColNames": [],
            "skewedColValueLocationMaps": {},
            "skewedColValues": []
        },
        "sortCols": [],
        "storedAsSubDirectories": false
    },
    "tableName": "mytable",
    "values": [
        "d1",
        "t1"
    ]
}
```

### Deleting partition
