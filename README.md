### Spark datasource example 

Sample code which demonstrate how to use Spark datasource API to read and write data from/to external data sources.
Here LMDB is used as storage engine since it is a key-value store which is representative for many other data sources in the big data world.
Starting point is the  [TestDsReadAndRewrite.java](src%2Ftest%2Fjava%2Forg%2Fds%2Fdsv2%2FTestDsReadAndRewrite.java) test class.

```bash
mvn clean test
```