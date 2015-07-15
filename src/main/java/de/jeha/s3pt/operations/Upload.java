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

/**
 * @author jenshadlich@googlemail.com
 */
public class Upload extends AbstractOperation {

    private final static Logger LOG = LoggerFactory.getLogger(Upload.class);

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int n;
    private final int size;

    public Upload(AmazonS3 s3Client, String bucketName, int n, int size) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.n = n;
        this.size = size;
    }

    @Override
    public OperationResult call() {
        LOG.info("Upload: n={}, size={} byte", n, size);

        final byte data[] = RandomDataGenerator.generate(size);

        for (int i = 0; i < n; i++) {
            final String key = UUID.randomUUID().toString();
            LOG.debug("Uploading file: {}", key);

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(data.length);

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, new ByteArrayInputStream(data), objectMetadata);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            s3Client.putObject(putObjectRequest);

            stopWatch.stop();

            LOG.debug("Time = {} ms", stopWatch.getTime());
            getStats().addValue(stopWatch.getTime());

            if (i > 0 && i % 1000 == 0) {
                LOG.info("Progress: {} of {}", i, n);
            }
        }

        return new OperationResult(getStats());
    }
}
