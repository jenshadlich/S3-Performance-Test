package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author jenshadlich@googlemail.com
 */
public class RandomRead implements Runnable {

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

        LOG.info("Collect keys for test");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int filesRead = 0;
        Map<Integer, String> files = new HashMap<>();
        for (S3ObjectSummary objectSummary : s3Client.listObjects(bucketName).getObjectSummaries()) {
            filesRead++;
            files.put(filesRead, objectSummary.getKey());
        }
        stopWatch.stop();

        LOG.info("Time = {} ms", stopWatch.getTime());

        LOG.info("Files read for test: {}", filesRead);

        for (int i = 0; i < n; i++) {
            final String randomKey= files.get(GENERATOR.nextInt(filesRead));
            LOG.info("Read file: {}", randomKey);

            stopWatch = new StopWatch();
            stopWatch.start();

            s3Client.getObject(bucketName, randomKey);

            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());
        }


        LOG.info("Time = {} ms", stopWatch.getTime());
    }

}
