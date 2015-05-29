package de.jeha.s3pt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import de.jeha.s3pt.operations.AbstractOperation;
import de.jeha.s3pt.operations.ClearBucket;
import de.jeha.s3pt.operations.RandomRead;
import de.jeha.s3pt.operations.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

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
    private final int threads;
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
                             Operation operation, int threads, int n, int size) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.bucketName = bucketName;
        this.operation = operation;
        this.threads = threads;
        this.n = n;
        this.size = size;
    }

    @Override
    public void run() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = new AmazonS3Client(credentials);
        s3Client.setEndpoint(endpointUrl);

        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        List<Callable<OperationResult>> operations = new ArrayList<>();
        if (operation.isMultiThreaded()) {
            for (int i = 0; i < threads; i++) {
                operations.add(createOperation(operation, s3Client));
            }
        } else {
            if (threads > 1) {
                LOG.warn("operation {} does not support multiple threads, use single thread", operation);
            }
            operations.add(createOperation(operation, s3Client));
        }

        try {
            List<Future<OperationResult>> futureResults = executorService.invokeAll(operations);

            List<OperationResult> results = new ArrayList<>();
            for (Future<OperationResult> result : futureResults) {
                results.add(result.get());
            }
            results.forEach(this::printResult);

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("An error occurred", e);
        }

        executorService.shutdown();

        LOG.info("Done");
    }

    private AbstractOperation createOperation(Operation operation, AmazonS3 s3Client) {
        switch (operation) {
            case UPLOAD:
                return new Upload(s3Client, bucketName, n, size);
            case CLEAR_BUCKET:
                return new ClearBucket(s3Client, bucketName, n);
            case RANDOM_READ:
                return new RandomRead(s3Client, bucketName, n);
            default:
                throw new UnsupportedOperationException("Unknown operation: " + operation);
        }
    }

    protected void printResult(OperationResult result) {
        LOG.info("Request statistics:");
        LOG.info("min = {} ms", (int) result.getStatistics().getMin());
        LOG.info("max = {} ms", (int) result.getStatistics().getMax());
        LOG.info("avg = {} ms", (int) result.getStatistics().getGeometricMean());
        LOG.info("p50 = {} ms", (int) result.getStatistics().getPercentile(50));
        LOG.info("p75 = {} ms", (int) result.getStatistics().getPercentile(75));
        LOG.info("p95 = {} ms", (int) result.getStatistics().getPercentile(95));
        LOG.info("p98 = {} ms", (int) result.getStatistics().getPercentile(98));
        LOG.info("p99 = {} ms", (int) result.getStatistics().getPercentile(99));
        LOG.info("throughput = {} req/s", result.getStatistics().getSum() / result.getStatistics().getN());
    }

}
