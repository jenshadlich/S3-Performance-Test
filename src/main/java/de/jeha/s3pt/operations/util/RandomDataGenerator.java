package de.jeha.s3pt.operations.util;

import java.util.Random;

/**
 * @author jenshadlich@googlemail.com
 */
public class RandomDataGenerator {

    private final static Random GENERATOR = new Random();

    public static byte[] generate(int size) {
        final byte data[] = new byte[size];
        GENERATOR.nextBytes(data);
        return data;
    }

}
