package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import de.jeha.s3pt.OperationResult;
import de.jeha.s3pt.operations.util.RandomDataGenerator;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author jenshadlich@googlemail.com
 */
public class UploadAndRead extends AbstractOperation {

    private final static Logger LOG = LoggerFactory.getLogger(UploadAndRead.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final String prefix;
    private final int n;
    private final int size;

    public UploadAndRead(AmazonS3 s3Client, String bucket, String prefix, int n, int size) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.n = n;
        this.size = size;
    }

    @Override
    public OperationResult call() {
        LOG.info("Upload: n={}, size={} byte", n, size);

        final byte[] readBuffer = new byte[4096];

        for (int i = 0; i < n; i++) {
            final byte[] data = RandomDataGenerator.generate(size);
            final String key;
            if (prefix != null) {
                key = prefix + "/" + UUID.randomUUID().toString();
            } else {
                key = UUID.randomUUID().toString();
            }

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(data.length);

            final PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucket, key, new ByteArrayInputStream(data), objectMetadata);

            final StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            s3Client.putObject(putObjectRequest);

            try (S3Object object = s3Client.getObject(bucket, key)) {
                int totalRead = 0;
                try (S3ObjectInputStream inputStream = object.getObjectContent()) {
                    int readLength;
                    while ((readLength = inputStream.read(readBuffer)) >= 0) {
                        totalRead += readLength;
                    }
                }
                if (totalRead != data.length) {
                    LOG.warn("Upload/read size mismatch for key {}: uploaded={} bytes, read={} bytes",
                            key, data.length, totalRead);
                }
            } catch (IOException e) {
                LOG.warn("An exception occurred while reading with key: {}", key);
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
