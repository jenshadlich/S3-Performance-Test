package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import de.jeha.s3pt.OperationResult;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class ClearBucket extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ClearBucket.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final int n;

    public ClearBucket(AmazonS3 s3Client, String bucket, int n) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.n = n;
    }

    @Override
    public OperationResult call() throws Exception {
        LOG.info("Clear bucket: bucket={}, n={}", bucket, n);

        int deleted = 0;
        boolean truncated;
        do {
            ObjectListing objectListing = s3Client.listObjects(bucket);
            truncated = objectListing.isTruncated();

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                LOG.debug("Delete object: {}, #deleted {}", objectSummary.getKey(), deleted);

                StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                s3Client.deleteObject(bucket, objectSummary.getKey());

                stopWatch.stop();

                LOG.debug("Time = {} ms", stopWatch.getTime());
                getStats().addValue(stopWatch.getTime());

                deleted++;
                if (deleted >= n) {
                    break;
                }
                if (deleted % 1000 == 0) {
                    LOG.info("Objects deleted so far: {}", deleted);
                }
            }
        } while (truncated && deleted < n);

        LOG.info("Object deleted: {}", deleted);

        return new OperationResult(getStats());
    }
}
