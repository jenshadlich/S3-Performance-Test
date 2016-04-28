package de.jeha.s3pt;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import de.jeha.s3pt.operations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author jenshadlich@googlemail.com
 */
public class S3PerformanceTest implements Callable<TestResult> {

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
    private final String signerOverride;
    private final boolean useKeepAlive;
    private final boolean usePathStyleAccess;
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
     * @param signerOverride override the S3 signer
     * @param useKeepAlive   use TCP keep alive
     * @param keyFileName    name of file with object keys
     */
    public S3PerformanceTest(String accessKey, String secretKey, String endpointUrl, String bucketName,
                             Operation operation, int threads, int n, int size, boolean useHttp, boolean useGzip,
                             String signerOverride, boolean useKeepAlive, boolean usePathStyleAccess,
                             String keyFileName) {
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
        this.signerOverride = signerOverride;
        this.useKeepAlive = useKeepAlive;
        this.usePathStyleAccess = usePathStyleAccess;
        this.keyFileName = keyFileName;
    }

    @Override
    public TestResult call() {
        AmazonS3 s3Client = buildS3Client();

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

        TestResult testResult = null;
        try {
            List<Future<OperationResult>> futureResults = executorService.invokeAll(operations);

            List<OperationResult> operationResults = new ArrayList<>();
            for (Future<OperationResult> result : futureResults) {
                operationResults.add(result.get());
            }

            testResult = TestResult.compute(operationResults);

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("An error occurred", e);
        }

        executorService.shutdown();

        LOG.info("Done");

        return testResult;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * @return S3 client
     */
    private AmazonS3 buildS3Client() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(useHttp ? Protocol.HTTP : Protocol.HTTPS)
                .withUserAgent("s3pt")
                .withGzip(useGzip)
                .withTcpKeepAlive(useKeepAlive);

        if (signerOverride != null) {
            String signer = signerOverride.endsWith("Type")
                    ? signerOverride
                    : signerOverride + "Type";
            clientConfiguration.setSignerOverride(signer);
        }

        AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfiguration);
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(usePathStyleAccess).disableChunkedEncoding().build());
        s3Client.setEndpoint(endpointUrl);

        return s3Client;
    }

    /**
     * Build the given operation.
     *
     * @param operation operation (enum)
     * @param s3Client  S3 client
     * @return operation
     */
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

}
