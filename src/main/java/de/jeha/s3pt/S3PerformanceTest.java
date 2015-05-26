package de.jeha.s3pt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
    private final TestMode testMode;
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
    public S3PerformanceTest(String accessKey, String secretKey, String endpointUrl, String bucketName,
                             TestMode testMode, int n, int size) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.bucketName = bucketName;
        this.testMode = testMode;
        this.n = n;
        this.size = size;
    }

    @Override
    public void run() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3 = new AmazonS3Client(credentials);

        s3.setEndpoint(endpointUrl);

        switch (testMode) {
            case UPLOAD:
                upload(s3);
                break;
            case CLEAR_BUCKET:
                clearBucket(s3);
                break;
            default:
                throw new UnsupportedOperationException("Unknown mode: " + testMode);
        }

        LOG.info("Done");
    }

    private void clearBucket(AmazonS3 s3) {
        LOG.info("Clear bucket: n={}", n);

        int deleted = 0;
        for (S3ObjectSummary objectSummary : s3.listObjects(bucketName).getObjectSummaries()) {
            LOG.info("Delete file: {}", objectSummary.getKey());

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            s3.deleteObject(bucketName, objectSummary.getKey());
            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());

            deleted++;
            if (deleted >= n) {
                break;
            }
        }

        LOG.info("Files deleted: {}", deleted);
    }

    private void upload(AmazonS3 s3) {
        LOG.info("Upload: n={}, size={} byte", n, size);

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
    }
}
