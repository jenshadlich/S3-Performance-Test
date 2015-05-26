package de.jeha.s3pt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Random;
import java.util.UUID;

/**
 * @author jenshadlich@googlemail.com
 */
public class S3PerformanceTest implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(S3PerformanceTest.class);
    private final static Random GENERATOR = new Random();

    private final String accessKey;
    private final String secretKey;
    private final String endpointUrl;
    private final String bucketName;
    private final int n;
    private final int size;

    /**
     * @param accessKey   access key
     * @param secretKey   secret key
     * @param endpointUrl endpoint url
     * @param bucketName  bucket name
     * @param n           number of operations
     * @param size        size for upload operations
     */
    public S3PerformanceTest(String accessKey, String secretKey, String endpointUrl, String bucketName, int n, int size) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.bucketName = bucketName;
        this.n = n;
        this.size = size;
    }

    @Override
    public void run() {
        LOG.info("Run test: n={}, size={} byte", n, size);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3 = new AmazonS3Client(credentials);

        s3.setEndpoint(endpointUrl);

        // create some random data
        final byte data[] = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) GENERATOR.nextInt(255);
        }

        for (int i = 0; i < n; i++) {
            final String key = UUID.randomUUID().toString();
            LOG.info("Uploading file: {}", key);

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(data.length);

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, new ByteArrayInputStream(data), objectMetadata);

            StopWatch stopWatch = new StopWatch();

            stopWatch.start();
            s3.putObject(putObjectRequest);
            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());
        }

        LOG.info("Done");
    }
}
