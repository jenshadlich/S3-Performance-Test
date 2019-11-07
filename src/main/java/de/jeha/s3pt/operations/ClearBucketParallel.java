package de.jeha.s3pt.operations;

import com.amazonaws.services.s3.AmazonS3;
import de.jeha.s3pt.OperationResult;
import de.jeha.s3pt.operations.data.ObjectKeys;
import de.jeha.s3pt.operations.data.S3ObjectKeysDataProvider;
import de.jeha.s3pt.operations.data.SingletonFileObjectKeysDataProvider;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

public class ClearBucketParallel extends AbstractOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ClearBucketParallel.class);

    private final AmazonS3 s3Client;
    private final String bucket;
    private final String prefix;
    private final int n;
    private final String keyFileName;
    private final int threads;

    public ClearBucketParallel(AmazonS3 s3Client, String bucket, String prefix, int n, String keyFileName, int threads) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.prefix = prefix;
        this.n = n;
        this.keyFileName = keyFileName;
        this.threads = threads;
    }

    @Override
    public OperationResult call() {
        LOG.info("Clear bucket parallel: bucket={}, keyFileName={}, n={}, threads={}", bucket, keyFileName, n, threads);

        final ObjectKeys objectKeys;
        if (keyFileName == null) {
            objectKeys = new S3ObjectKeysDataProvider(s3Client, bucket, prefix).get();
        } else {
            objectKeys = new SingletonFileObjectKeysDataProvider(keyFileName).get();
        }
        final int objectsMaxIndex = Math.min(n, objectKeys.size());
        LOG.info("Objects to be deleted: {}", objectsMaxIndex);

        InheritableThreadLocal<Integer> counter = new InheritableThreadLocal<>();
        counter.set(0);
        ForkJoinPool forkJoinPool = null;
        try {
            forkJoinPool = new ForkJoinPool(threads);
            forkJoinPool.submit(() -> IntStream.range(0, objectsMaxIndex).parallel().forEach(i -> {

                final String key = objectKeys.get(i);
                final StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                s3Client.deleteObject(bucket, key);
                stopWatch.stop();
                getStats().addValue(stopWatch.getTime());

                final int c = counter.get();
                if (c % 1000 == 0) {
                    LOG.info("Progress thread {}: {} of {}", Thread.currentThread().getId(), c, objectsMaxIndex / threads);
                }
                counter.set(c+1);
            })).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }
        return new OperationResult(getStats());
    }

    public void call(ObjectKeys objectKeys) {

        final String key = objectKeys.getRandom();
        s3Client.deleteObject(bucket, key);
    }
}
