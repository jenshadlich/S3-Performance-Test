package de.jeha.s3pt;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import de.jeha.s3pt.operations.*;
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

            printResults(results);

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
            case CREATE_BUCKET:
                return new CreateBucket(s3Client, bucketName);
            case CLEAR_BUCKET:
                return new ClearBucket(s3Client, bucketName, n);
            case RANDOM_READ:
                return new RandomRead(s3Client, bucketName, n);
            default:
                throw new UnsupportedOperationException("Unknown operation: " + operation);
        }
    }

    private void printResults(List<OperationResult> results) {
        int min = (int) results.stream().mapToDouble(x -> x.getStats().getMin()).average().orElse(0.0);
        int max = (int) results.stream().mapToDouble(x -> x.getStats().getMax()).average().orElse(0.0);
        int avg = (int) results.stream().mapToDouble(x -> x.getStats().getGeometricMean()).average().orElse(0.0);
        int p50 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(50)).average().orElse(0.0);
        int p75 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(75)).average().orElse(0.0);
        int p95 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(95)).average().orElse(0.0);
        int p98 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(98)).average().orElse(0.0);
        int p99 = (int) results.stream().mapToDouble(x -> x.getStats().getPercentile(99)).average().orElse(0.0);
        double throughput = results.stream().mapToDouble(x -> x.getStats().getSum() / x.getStats().getN()).sum();

        LOG.info("Request statistics:");
        LOG.info("min = {} ms", min);
        LOG.info("max = {} ms", max);
        LOG.info("avg = {} ms", avg);
        LOG.info("p50 = {} ms", p50);
        LOG.info("p75 = {} ms", p75);
        LOG.info("p95 = {} ms", p95);
        LOG.info("p98 = {} ms", p98);
        LOG.info("p99 = {} ms", p99);
        LOG.info("throughput = {} req/s ({} threads)", throughput, threads);
    }

}
