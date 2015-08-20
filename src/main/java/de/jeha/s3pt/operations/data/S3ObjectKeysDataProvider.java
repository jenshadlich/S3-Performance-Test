package de.jeha.s3pt.operations.data;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class S3ObjectKeysDataProvider implements DataProvider<ObjectKeys> {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectKeysDataProvider.class);

    private final AmazonS3 s3Client;
    private final String bucket;

    public S3ObjectKeysDataProvider(AmazonS3 s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public ObjectKeys get() {
        int objectsRead = 0;
        ObjectKeys objectKeys = new ObjectKeys();

        LOG.info("Collect object keys");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        boolean truncated;
        ObjectListing previousObjectListing = null;
        do {
            ObjectListing objectListing = (previousObjectListing != null)
                    ? s3Client.listNextBatchOfObjects(previousObjectListing)
                    : s3Client.listObjects(bucket);
            previousObjectListing = objectListing;
            truncated = objectListing.isTruncated();

            LOG.debug("Loaded {} objects", objectListing.getObjectSummaries().size());

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                objectKeys.add(objectSummary.getKey());
                objectsRead++;
            }

        } while (truncated && objectsRead < 1000);

        stopWatch.stop();

        LOG.info("Time = {} ms", stopWatch.getTime());
        LOG.info("Objects read: {}, object keys available: {}", objectsRead, objectKeys.size());

        return objectKeys;
    }

}
