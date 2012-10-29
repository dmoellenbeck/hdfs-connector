# Mule HDFS Connector

<img src="http://hadoop.apache.org/docs/hdfs/current/images/hdfs-logo.jpg" />

## Testing

By default, tests that are run with:

    mvn test

are against the local file system, effectively validating the connector's capacity to interact with an `org.apache.hadoop.fs.FileSystem`.

To run the tests on the default local HDFS setup (ie. `hdfs://localhost:8020/`), use:

    mvn -Pit test

All what this profile does is setting the following Java system properties:

- `test.hdfs.fs.default.name` to `hdfs://localhost:8020/`
- `test.hdfs.temp.dir` to `/tmp`

To test against a different HDFS setup, use `mvn test` while setting appropriate values for the above system properties.
