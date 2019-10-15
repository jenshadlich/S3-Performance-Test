package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import de.jeha.s3pt.OperationResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author jenshadlich@googlemail.com
 */
public class CreateKeyFile extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateKeyFile.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final String prefix;
    private final int n;
    private final String keyFileName;

    public CreateKeyFile(AmazonS3 s3Client, String bucket, String prefix, int n, String keyFileName) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.n = n;
        this.keyFileName = keyFileName;
    }

    @Override
    public OperationResult call() throws IOException {
        LOG.info("Create key file: n={}", n);
        LOG.info("Start collecting object keys");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int objectsRead = 0;

        File keyFile = new File(keyFileName);
        FileUtils.writeStringToFile(keyFile, "", StandardCharsets.UTF_8);

        boolean truncated;
        ObjectListing previousObjectListing = null;
        do {
            ObjectListing objectListing = (previousObjectListing != null)
                    ? s3Client.listNextBatchOfObjects(previousObjectListing)
                    : s3Client.listObjects(bucket, prefix);
            previousObjectListing = objectListing;
            truncated = objectListing.isTruncated();

            LOG.debug("Loaded {} objects", objectListing.getObjectSummaries().size());

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                FileUtils.writeStringToFile(keyFile, objectSummary.getKey(), StandardCharsets.UTF_8, true);
                FileUtils.writeStringToFile(keyFile, System.lineSeparator(), StandardCharsets.UTF_8, true);
                objectsRead++;
                if (objectsRead >= n) {
                    break;
                }
            }

            LOG.info("Progress: {} of {}", objectsRead, n);

        } while (truncated && objectsRead < n);

        stopWatch.stop();
        getStats().addValue(stopWatch.getTime());

        LOG.info("Time = {} ms", stopWatch.getTime());

        return new OperationResult(getStats());
    }

}
