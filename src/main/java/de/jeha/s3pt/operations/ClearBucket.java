package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class ClearBucket extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ClearBucket.class);

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int n;

    public ClearBucket(AmazonS3 s3Client, String bucketName, int n) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.n = n;
    }

    @Override
    public void run() {
        LOG.info("Clear bucket: n={}", n);

        // TODO: properly support pagination
        int deleted = 0;
        for (S3ObjectSummary objectSummary : s3Client.listObjects(bucketName).getObjectSummaries()) {
            LOG.info("Delete file: {}", objectSummary.getKey());

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            s3Client.deleteObject(bucketName, objectSummary.getKey());

            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());
            getStatistics().addValue(stopWatch.getTime());

            deleted++;
            if (deleted >= n) {
                break;
            }
        }

        LOG.info("Files deleted: {}", deleted);
        logStatistics();
    }

}
