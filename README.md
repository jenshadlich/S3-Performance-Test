# S3 Performance Test Tool
Performance test tool for Amazon S3 or S3-compatible object storage like Ceph with radosgw.

##### Build:
(requires Java 8 or higher)
```
mvn clean install
```

##### Usage by example:

###### UPLOAD of n randomly generated files (key = UUID), each 2kB size
```
java -jar target/s3-pt.jar --accessKey <accessKey> --secretKey <secretKey> --bucketName <bucketName> -n <number of files to upload> -s 2048
```

###### RANDOM_READ with 4 parallel threads, each 10.000 reads = 40.000 requests
```
java -jar target/s3-pt.jar --accessKey <accessKey> --secretKey <secretKey> --bucketName <bucketName> --operation=RANDOM_READ -n 10000 -t 4
```

###### General usage:

```
java -jar s3-pt.jar [options...]
 --accessKey VAL   : access key ID
 --bucketName VAL  : name of bucket
 --endpointUrl VAL : endpoint url (default: s3.amazonaws.com)
 --secretKey VAL   : secret access key
 --size N          : number of files (default: 131072)
 -n N              : number of operations
```

To print the usage information execute `java -jar target/s3-pt.jar` on the command line.
