package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import de.jeha.s3pt.OperationResult;
import de.jeha.s3pt.operations.data.ObjectKeys;
import de.jeha.s3pt.operations.data.S3ObjectKeysDataProvider;
import de.jeha.s3pt.operations.data.SingletonFileObjectKeysDataProvider;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class RandomReadMetadata extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(RandomReadMetadata.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final String prefix;
    private final int n;
    private final String keyFileName;

    public RandomReadMetadata(AmazonS3 s3Client, String bucket, String prefix, int n, String keyFileName) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.n = n;
        this.keyFileName = keyFileName;
    }

    @Override
    public OperationResult call() {
        LOG.info("Random read metadata: n={}", n);
        final ObjectKeys objectKeys;
        if (keyFileName == null) {
            objectKeys = new S3ObjectKeysDataProvider(s3Client, bucket, prefix).get();
        } else {
            objectKeys = new SingletonFileObjectKeysDataProvider(keyFileName).get();
        }
        final StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < n; i++) {
            final String randomKey = objectKeys.getRandom();
            stopWatch.reset();
            stopWatch.start();
            final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucket, randomKey);
            LOG.debug("Object version: {}", objectMetadata.getVersionId());
            stopWatch.stop();
            getStats().addValue(stopWatch.getTime());
            if (i > 0 && i % 1000 == 0) {
                LOG.info("Progress: {} of {}", i, n);
            }
        }
        return new OperationResult(getStats());
    }
}
