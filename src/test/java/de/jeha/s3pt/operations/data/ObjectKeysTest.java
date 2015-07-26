package de.jeha.s3pt.operations.data;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author jenshadlich@googlemail.com
 */
public class ObjectKeysTest {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectKeysTest.class);

    @Test
    public void testEmpty() {
        ObjectKeys objectKeys = new ObjectKeys();

        assertNull(objectKeys.getRandom());
    }

    @Test
    public void testOne() {
        ObjectKeys objectKeys = new ObjectKeys();
        objectKeys.add("1");

        assertEquals("1", objectKeys.getRandom());
    }

    @Test
    public void testThree() {
        ObjectKeys objectKeys = new ObjectKeys();
        objectKeys.add("1");
        objectKeys.add("2");
        objectKeys.add("3");

        int one = 0;
        int two = 0;
        int three = 0;

        for (int i = 0; i < 100; i++) {
            String key = objectKeys.getRandom();

            switch (key) {
                case "1":
                    one++;
                    break;
                case "2":
                    two++;
                    break;
                case "3":
                    three++;
                    break;
                default:
                    fail("unexpected key");
            }
        }

        LOG.info("1 = {}, 2 = {}, 3 = {}", one, two, three);

        assertTrue(one > 10);
        assertTrue(two > 10);
        assertTrue(three > 10);
    }

}
