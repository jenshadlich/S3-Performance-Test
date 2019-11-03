package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import de.jeha.s3pt.OperationResult;
import de.jeha.s3pt.operations.data.ObjectKeys;
import de.jeha.s3pt.operations.data.S3ObjectKeysDataProvider;
import de.jeha.s3pt.operations.data.SingletonFileObjectKeysDataProvider;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RandomReadFirstByte extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(RandomReadFirstByte.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final String prefix;
    private final int n;
    private final String keyFileName;

    public RandomReadFirstByte(AmazonS3 s3Client, String bucket, String prefix, int n, String keyFileName) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.n = n;
        this.keyFileName = keyFileName;
    }

    @Override
    public OperationResult call() {
        LOG.info("Random read first byte: n={}", n);
        final ObjectKeys objectKeys;
        if (keyFileName == null) {
            objectKeys = new S3ObjectKeysDataProvider(s3Client, bucket, prefix).get();
        } else {
            objectKeys = new SingletonFileObjectKeysDataProvider(keyFileName).get();
        }
        final StopWatch stopWatch = new StopWatch();
        final byte[] readBuffer = new byte[4096];
        for (int i = 0; i < n; i++) {
            final String randomKey = objectKeys.getRandom();
            stopWatch.reset();
            stopWatch.start();
            try (S3Object object = s3Client.getObject(bucket, randomKey)) {
                try (S3ObjectInputStream inputStream = object.getObjectContent()) {
                    inputStream.read(readBuffer);
                    inputStream.abort();
                    //IOUtils.drainInputStream(inputStream);
                }
            } catch (IOException e) {
                LOG.warn("An exception occurred while reading with key: {}", randomKey);
            }
            stopWatch.stop();
            getStats().addValue(stopWatch.getTime());
            if (i > 0 && i % 1000 == 0) {
                LOG.info("Progress: {} of {}", i, n);
            }
        }
        return new OperationResult(getStats());
    }
}
