package de.jeha.s3pt.operations.util;

import java.io.InputStream;

public class RandomInputStream extends InputStream {
    private long remainingBytes;

    public RandomInputStream(long size) {
        this.remainingBytes = size;
    }

    private byte[] generate(int len) {
        int generateSize = (int) Math.min(remainingBytes, (long) len);
        byte[] generated;
        if (generateSize > 0) {
            generated = RandomDataGenerator.generate(generateSize);
            remainingBytes -= generateSize;
        } else {
            generated = new byte[0];
        }
        return generated;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        byte[] g = generate(len);
        if (g.length > 0) {
            System.arraycopy(g, 0, b, off, g.length);
            return g.length;
        } else {
            return -1;
        }
    }

    @Override
    public int read() {
        byte[] b = generate(1);
        return b.length > 0 ? b[0] & 255 : -1;
    }

    @Override
    public int available() {
        return (int) remainingBytes;
    }
}
