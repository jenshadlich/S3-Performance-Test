package de.jeha.s3pt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import de.jeha.s3pt.operations.ClearBucket;
import de.jeha.s3pt.operations.RandomRead;
import de.jeha.s3pt.operations.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class S3PerformanceTest implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(S3PerformanceTest.class);

    private final String accessKey;
    private final String secretKey;
    private final String endpointUrl;
    private final String bucketName;
    private final Operation operation;
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
                             Operation operation, int n, int size) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.bucketName = bucketName;
        this.operation = operation;
        this.n = n;
        this.size = size;
    }

    @Override
    public void run() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = new AmazonS3Client(credentials);

        s3Client.setEndpoint(endpointUrl);

        // TODO: use a factory
        switch (operation) {
            case UPLOAD:
                new Upload(s3Client, bucketName, n, size).run();
                break;
            case CLEAR_BUCKET:
                new ClearBucket(s3Client, bucketName, n).run();
                break;
            case RANDOM_READ:
                new RandomRead(s3Client, bucketName, n).run();
                break;
            default:
                throw new UnsupportedOperationException("Unknown mode: " + operation);
        }

        LOG.info("Done");
    }

}
