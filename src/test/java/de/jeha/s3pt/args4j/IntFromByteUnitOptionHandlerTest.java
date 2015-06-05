package de.jeha.s3pt.args4j;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author jenshadlich@googlemail.com
 */
public class IntFromByteUnitOptionHandlerTest {

    private final IntFromByteUnitOptionHandler handler = new IntFromByteUnitOptionHandler(null, null, null);

    @Test
    public void test() {
        assertEquals(1, (long) handler.parse("1"));
        assertEquals(1, (long) handler.parse("1B"));
        assertEquals(1, (long) handler.parse("1b"));
        assertEquals(1024, (long) handler.parse("1024"));
        assertEquals(1024, (long) handler.parse("1K"));
        assertEquals(2048, (long) handler.parse("2k"));
        assertEquals(1024 * 1024, (long) handler.parse("1M"));
        assertEquals(1024 * 1024, (long) handler.parse("1048576"));
    }

    @Test(expected = NumberFormatException.class)
    public void testIllegalNumber() {
        handler.parse("a");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalUnit() {
        handler.parse("1G");
    }

}
