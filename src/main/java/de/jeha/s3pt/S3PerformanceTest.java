package de.jeha.s3pt;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
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
    private final boolean useHttp;
    private final boolean useGzip;
    private final boolean useOldS3Signer;
    private final boolean useKeepAlive;
    private final String keyFileName;

    /**
     * @param accessKey      access key
     * @param secretKey      secret key
     * @param endpointUrl    endpoint url, e.g. 's3.amazonaws.com'
     * @param bucketName     name of bucket
     * @param operation      operation
     * @param threads        number of threads
     * @param n              number of operations
     * @param size           size (if applicable), e.g. for UPLOAD operation
     * @param useHttp        switch to HTTP when
     * @param useGzip        enable GZIP compression
     * @param useOldS3Signer use "old" S3Signer for endpoints that do not support v4 signing
     * @param useKeepAlive   use TCP keep alive
     * @param keyFileName    name of file with object keys
     */
    public S3PerformanceTest(String accessKey, String secretKey, String endpointUrl, String bucketName,
                             Operation operation, int threads, int n, int size, boolean useHttp, boolean useGzip,
                             boolean useOldS3Signer, boolean useKeepAlive, String keyFileName) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.bucketName = bucketName;
        this.operation = operation;
        this.threads = threads;
        this.n = n;
        this.size = size;
        this.useHttp = useHttp;
        this.useGzip = useGzip;
        this.useOldS3Signer = useOldS3Signer;
        this.useKeepAlive = useKeepAlive;
        this.keyFileName = keyFileName;
    }

    @Override
    public void run() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration()
                .withProtocol(useHttp ? Protocol.HTTP : Protocol.HTTPS)
                .withUserAgent("s3pt")
                .withGzip(useGzip)
                .withTcpKeepAlive(useKeepAlive);

        if (useOldS3Signer) {
            clientConfig.setSignerOverride(Constants.S3_SIGNER_TYPE);
        }

        AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfig);
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
            case CLEAR_BUCKET:
                return new ClearBucket(s3Client, bucketName, n);
            case CREATE_BUCKET:
                return new CreateBucket(s3Client, bucketName);
            case CREATE_KEY_FILE:
                return new CreateKeyFile(s3Client, bucketName, n, keyFileName);
            case RANDOM_READ:
                return new RandomRead(s3Client, bucketName, n, keyFileName);
            case RANDOM_READ_METADATA:
                return new RandomReadMetadata(s3Client, bucketName, n, keyFileName);
            case UPLOAD:
                return new Upload(s3Client, bucketName, n, size);
            case UPLOAD_AND_READ:
                return new UploadAndRead(s3Client, bucketName, n, size);
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
        double ops = results.stream().mapToDouble(x -> x.getStats().getN() / x.getStats().getSum() * 1000).sum();

        LOG.info("Operation statistics:");
        LOG.info("min = {} ms", min);
        LOG.info("max = {} ms", max);
        LOG.info("avg = {} ms", avg);
        LOG.info("p50 = {} ms", p50);
        LOG.info("p75 = {} ms", p75);
        LOG.info("p95 = {} ms", p95);
        LOG.info("p98 = {} ms", p98);
        LOG.info("p99 = {} ms", p99);
        LOG.info("throughput = {} operations/s ({} threads)", (int) ops, threads);
    }

}
