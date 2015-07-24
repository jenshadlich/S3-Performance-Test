package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import de.jeha.s3pt.OperationResult;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author jenshadlich@googlemail.com
 */
public class RandomRead extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(RandomRead.class);
    private final static Random GENERATOR = new Random();

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int n;

    public RandomRead(AmazonS3 s3Client, String bucketName, int n) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.n = n;
    }

    @Override
    public OperationResult call() {
        LOG.info("Random read: n={}", n);

        LOG.info("Collect objects for test");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int filesRead = 0;
        Map<Integer, String> files = new HashMap<>();
        //Set<String> keys = new HashSet<>();

        boolean truncated;
        ObjectListing previousObjectListing = null;
        do {
            ObjectListing objectListing = (previousObjectListing != null)
                    ? s3Client.listNextBatchOfObjects(previousObjectListing)
                    : s3Client.listObjects(bucketName);
            previousObjectListing = objectListing;
            truncated = objectListing.isTruncated();

            LOG.debug("Loaded {} objects", objectListing.getObjectSummaries().size());

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                //keys.add(objectSummary.getKey());
                files.put(filesRead, objectSummary.getKey());
                filesRead++;
            }

        } while (truncated && filesRead < 100000);

        stopWatch.stop();

        LOG.info("Time = {} ms", stopWatch.getTime());

        LOG.info("Objects read for test: {}, files available: {}", filesRead, files.size());
        //LOG.info("Distinct keys: {}", keys.size());

        for (int i = 0; i < n; i++) {
            final String randomKey = (filesRead == 1)
                    ? files.get(0)
                    : files.get(GENERATOR.nextInt(files.size() - 1));
            LOG.debug("Read object: {}", randomKey);

            stopWatch = new StopWatch();
            stopWatch.start();

            S3Object object = s3Client.getObject(bucketName, randomKey);
            try {
                object.close();
            } catch (IOException e) {
                LOG.warn("An exception occurred while trying to close object with key: {}", randomKey);
            }

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
