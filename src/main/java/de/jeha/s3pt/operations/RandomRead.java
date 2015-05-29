package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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
    public void run() {
        LOG.info("Random read: n={}", n);

        LOG.info("Collect files for test");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int filesRead = 0;
        Map<Integer, String> files = new HashMap<>();
        // this should give a thousand objects
        // TODO: make it explicit and support pagination
        for (S3ObjectSummary objectSummary : s3Client.listObjects(bucketName).getObjectSummaries()) {
            files.put(filesRead, objectSummary.getKey());
            filesRead++;
        }
        stopWatch.stop();

        LOG.info("Time = {} ms", stopWatch.getTime());

        LOG.info("Files read for test: {}", filesRead);

        for (int i = 0; i < n; i++) {
            final String randomKey = files.get(GENERATOR.nextInt(files.size() - 1));
            LOG.info("Read file: {}", randomKey);

            stopWatch = new StopWatch();
            stopWatch.start();

            S3Object object = s3Client.getObject(bucketName, randomKey);
            try {
                object.close();
            } catch (IOException e) {
                LOG.warn("An exception occurred while trying to close object with key: {}", randomKey);
            }

            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());
            getStatistics().addValue(stopWatch.getTime());
        }

        logStatistics();
    }

}
