# S3 Performance Test Tool
Performance test tool for Amazon S3 or S3-compatible object storage like Ceph with radosgw.

##### Build:
(requires Java 7 or higher)
```
mvn clean install
```

##### Usage:
```
java -jar target/s3-pt.jar --accessKey <accessKey> --secretKey <secretKey> --bucketName <bucketName> -n <number of files to upload>
```

