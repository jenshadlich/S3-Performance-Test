# S3 Performance Test Tool

![build](https://api.travis-ci.org/jenshadlich/S3-Performance-Test.svg)

Performance test tool for Amazon S3 or S3-compatible object storage systems like Ceph with radosgw.

##### Build:
(requires Java 8 or higher)
```
mvn clean install
```

##### Usage by example:

###### UPLOAD of n randomly generated files (key = UUID), each 2kB size
```
java -jar target/s3pt.jar --accessKey <accessKey> --secretKey <secretKey> --bucketName <bucketName> -n <number of files to upload> --size 2048
```

###### RANDOM_READ with 4 parallel threads, each 10.000 reads = 40.000 requests
```
java -jar target/s3pt.jar --accessKey <accessKey> --secretKey <secretKey> --bucketName <bucketName> --operation=RANDOM_READ -n 10000 -t 4
```

###### General usage:

```
java -jar s3pt.jar [options...]
 --accessKey VAL      : access key ID; also possible to set AWS_ACCESS_KEY int
                        environment
 --bucketName VAL     : name of bucket
 --endpointUrl VAL    : endpoint url (default: s3.amazonaws.com)
 --gzip               : use gzip (default: false)
 --http               : use http instead of https (default: false)
 --keepAlive          : use TCP keep alive (default: false)
 --keyFileName VAL    : name of file with object keys
 --operation VAL      : operation (default: UPLOAD)
 --secretKey VAL      : secret access key; also possible to set AWS_SECRET_KEY
                        in environment
 --signerOverride VAL : override the S3 signer (e.g. 'S3Signer' or
                        'AWSS3V4Signer')
 --size N             : file size (e.g. for UPLOAD); supported units: B, K, M
                        (default: 131072)
 --usePathStyleAccess : use path-style access (instead of DNS-style) (default:
                        false)
 -n (--number) N      : number of operations
 -t (--threads) N     : number of threads (default: 1)
```

To print the usage information execute `java -jar target/s3pt.jar` on the command line.
