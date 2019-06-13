

package de.jeha.s3pt.operations;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.AmazonS3;
import de.jeha.s3pt.OperationResult;

/**
 * 
 * @author tanji11
 *
 */

public class DeleteBucket extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteBucket.class);

    private final AmazonS3 s3Client;
    private final String bucket;

    public DeleteBucket(AmazonS3 s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public OperationResult call() throws Exception {
        LOG.info("Delete bucket");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        s3Client.deleteBucket(bucket);

        stopWatch.stop();

        LOG.debug("Time = {} ms", stopWatch.getTime());
        getStats().addValue(stopWatch.getTime());

        return new OperationResult(getStats());
    }
}