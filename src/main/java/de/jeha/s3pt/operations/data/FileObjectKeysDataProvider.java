package de.jeha.s3pt.operations.data;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author jenshadlich@googlemail.com
 */
class FileObjectKeysDataProvider implements DataProvider<ObjectKeys> {

    private static final Logger LOG = LoggerFactory.getLogger(FileObjectKeysDataProvider.class);

    private final String fileName;
    private final ObjectKeys objectKeys = new ObjectKeys();

    public FileObjectKeysDataProvider(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public ObjectKeys get() {

        LOG.info("Collect object keys");

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)))) {
            reader.lines().forEach(objectKeys::add);
        } catch (IOException e) {
            LOG.error("Could not read key file", e);
        }

        stopWatch.stop();

        LOG.info("Time = {} ms", stopWatch.getTime());
        LOG.info("Object keys: {}", objectKeys.size());

        return objectKeys;
    }

}
