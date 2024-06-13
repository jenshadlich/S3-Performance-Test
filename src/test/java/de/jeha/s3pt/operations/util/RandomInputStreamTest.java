package de.jeha.s3pt.operations.util;

import junit.framework.TestCase;

import java.io.IOException;

public class RandomInputStreamTest extends TestCase {

    public void testReadArray() throws IOException {
        try (RandomInputStream inputStream = new RandomInputStream(25)) {
            byte[] b = new byte[10];
            assertEquals(10, inputStream.read(b, 0, 10));
            assertEquals(15, inputStream.available());
            assertEquals(5, inputStream.read(b, 0, 5));
            assertEquals(10, inputStream.read(b, 0, 15));
            assertEquals(-1, inputStream.read(b, 0, 10));
        }
    }

    public void testReadSingle() throws IOException {
        try (RandomInputStream inputStream = new RandomInputStream(25)) {
            for (int i = 0; i < 10; i++) {
                assertTrue("Read byte " +i, inputStream.read() >= 0);
            }
            assertEquals(15, inputStream.available());
            for (int i = 0; i < 15; i++) {
                assertTrue("Read byte " +(i+10), inputStream.read() >= 0);
            }
            assertTrue(inputStream.read() < 0);
        }
    }
}