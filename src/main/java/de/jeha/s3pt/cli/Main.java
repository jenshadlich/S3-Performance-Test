package de.jeha.s3pt.cli;

import de.jeha.s3pt.S3PerformanceTest;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.Locale;

/**
 * @author jenshadlich@googlemail.com
 */
public class Main {

    @Option(name = "-t", usage = "number of threads")
    private Integer threads = 1;

    @Option(name = "-n", usage = "number of request", required = true)
    private int n;

    @Option(name = "-s", usage = "number of files", required = true)
    private int size = 128 * 1024;

    @Argument(required = true)
    private String url;

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        new Main().run(args);
    }

    private void run(String... args) throws IOException {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("java -jar s3-pt.jar [options...] <url>");
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        new S3PerformanceTest().run();

        System.out.println("");
    }

}
