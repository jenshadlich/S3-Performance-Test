package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Random;
import java.util.UUID;

/**
 * @author jenshadlich@googlemail.com
 */
public class Upload implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(Upload.class);
    private final static Random GENERATOR = new Random();

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final int n;
    private final int size;

    public Upload(AmazonS3 s3Client, String bucketName, int n, int size) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.n = n;
        this.size = size;
    }

    @Override
    public void run() {
        LOG.info("Upload: n={}, size={} byte", n, size);

        // create some random data
        final byte data[] = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) GENERATOR.nextInt(255);
        }

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        for (int i = 0; i < n; i++) {
            final String key = UUID.randomUUID().toString();
            LOG.info("Uploading file: {}", key);

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(data.length);

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, new ByteArrayInputStream(data), objectMetadata);

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            s3Client.putObject(putObjectRequest);

            stopWatch.stop();

            LOG.info("Time = {} ms", stopWatch.getTime());
            statistics.addValue(stopWatch.getTime());
        }

        LOG.info("Request statistics:");
        LOG.info("min = {} ms", (int) statistics.getMin());
        LOG.info("max = {} ms", (int) statistics.getMax());
        LOG.info("avg = {} ms", (int) statistics.getGeometricMean());
        LOG.info("p50 = {} ms", (int) statistics.getPercentile(50));
        LOG.info("p75 = {} ms", (int) statistics.getPercentile(75));
        LOG.info("p95 = {} ms", (int) statistics.getPercentile(95));
        LOG.info("p98 = {} ms", (int) statistics.getPercentile(98));
        LOG.info("p99 = {} ms", (int) statistics.getPercentile(99));
    }
}
