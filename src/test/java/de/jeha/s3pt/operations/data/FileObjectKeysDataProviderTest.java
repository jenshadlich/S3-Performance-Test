package de.jeha.s3pt.operations.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author jenshadlich@googlemail.com
 */
public class FileObjectKeysDataProviderTest {

    @Test
    public void test() {
        ObjectKeys objectKeys = new FileObjectKeysDataProvider("src/test/resources/objectKeys.txt").get();

        assertEquals(3, objectKeys.size());

        assertEquals("1", objectKeys.get(0));
        assertEquals("2", objectKeys.get(1));
        assertEquals("3", objectKeys.get(2));
    }

}
