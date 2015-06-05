package de.jeha.s3pt.args4j;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OneArgumentOptionHandler;
import org.kohsuke.args4j.spi.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jenshadlich@googlemail.com
 */
public class IntFromByteUnitOptionHandler extends OneArgumentOptionHandler<Integer> {

    private static final Map<String, Integer> UNITS = new HashMap<>();

    static {
        UNITS.put("B", 1);
        UNITS.put("K", 1<<10);
        UNITS.put("M", 1<<20);
    }

    public IntFromByteUnitOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Integer> setter) {
        super(parser, option, setter);
    }

    @Override
    protected Integer parse(String argument) throws IllegalArgumentException {
        StringBuilder number = new StringBuilder();
        String unit = "B";

        for (int i = 0; i < argument.length(); i++) {
            char c = argument.charAt(i);
            if (!Character.isDigit(c)) {
                unit = argument.substring(i);
                break;
            }
            number.append(c);
        }

        final int i = Integer.parseInt(number.toString());

        if (!UNITS.containsKey(unit.toUpperCase())) {
            throw new IllegalArgumentException("Unit '" + unit + "' is not supported.");
        }

        final int factor = UNITS.get(unit.toUpperCase());

        return i * factor;
    }

}
