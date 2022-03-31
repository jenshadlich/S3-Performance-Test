package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import de.jeha.s3pt.OperationResult;
import de.jeha.s3pt.operations.util.RandomDataGenerator;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import static de.jeha.s3pt.operations.util.ProgressLogger.logProgress;

/**
 * @author jenshadlich@googlemail.com
 */
public class Upload extends AbstractOperation {

    private final static Logger LOG = LoggerFactory.getLogger(Upload.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final int n;
    private final int size;

    public Upload(AmazonS3 s3Client, String bucket, int n, int size) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.n = n;
        this.size = size;
    }

    @Override
    public OperationResult call() {
        LOG.info("Upload: n={}, size={} byte", n, size);
        for (int i = 0; i < n; i++) {
            final byte data[] = RandomDataGenerator.generate(size);
            final String key = UUID.randomUUID().toString();
            LOG.debug("Uploading object: {}", key);

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(data.length);

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucket, key, new ByteArrayInputStream(data), objectMetadata);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            s3Client.putObject(putObjectRequest);

            stopWatch.stop();

            LOG.debug("Time = {} ms", stopWatch.getTime());
            getStats().addValue(stopWatch.getTime());

            logProgress(LOG, i, n);
        }

        return new OperationResult(getStats());
    }
}
