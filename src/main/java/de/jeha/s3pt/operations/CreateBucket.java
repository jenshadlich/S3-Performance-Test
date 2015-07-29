package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import de.jeha.s3pt.OperationResult;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class CreateBucket extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBucket.class);

    private final AmazonS3 s3Client;
    private final String bucket;

    public CreateBucket(AmazonS3 s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public OperationResult call() throws Exception {
        LOG.info("Create bucket");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        s3Client.createBucket(bucket);

        stopWatch.stop();

        LOG.debug("Time = {} ms", stopWatch.getTime());
        getStats().addValue(stopWatch.getTime());

        return new OperationResult(getStats());
    }
}
