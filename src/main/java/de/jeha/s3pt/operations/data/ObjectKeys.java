package de.jeha.s3pt.operations.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author jenshadlich@googlemail.com
 */
public class ObjectKeys {

    private final static Random GENERATOR = new Random();

    private final Map<Integer, String> objectKeys = new HashMap<>();

    public void add(String key) {
        int sizeBefore = objectKeys.size(); // 0-based
        objectKeys.put(sizeBefore, key);
    }

    public int size() {
        return objectKeys.size();
    }

    public String get(Integer index) {
        return objectKeys.get(index);
    }

    public String getRandom() {
        if (objectKeys.size() == 0) {
            return null;
        } else {
            return objectKeys.get(GENERATOR.nextInt(objectKeys.size()));
        }
    }

}
