package de.jeha.s3pt.operations.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jenshadlich@googlemail.com
 */
public class SingletonFileObjectKeysDataProvider implements DataProvider<ObjectKeys> {

    private static final Logger LOG = LoggerFactory.getLogger(SingletonFileObjectKeysDataProvider.class);

    private static ObjectKeys OBJECT_KEYS = null;

    private final String fileName;

    public SingletonFileObjectKeysDataProvider(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public ObjectKeys get() {
        synchronized (SingletonFileObjectKeysDataProvider.class) {
            if (OBJECT_KEYS == null) {
                LOG.info("initialize object keys");
                OBJECT_KEYS = new FileObjectKeysDataProvider(fileName).get();
            }
        }
        return OBJECT_KEYS;
    }

}
