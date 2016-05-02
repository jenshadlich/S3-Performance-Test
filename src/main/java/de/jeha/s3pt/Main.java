package de.jeha.s3pt;

import de.jeha.s3pt.args4j.IntFromByteUnitOptionHandler;
import org.apache.commons.lang3.time.StopWatch;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * @author jenshadlich@googlemail.com
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String DEFAULT_S3_ENDPOINT = "s3.amazonaws.com";
    private static final String KEY_FILE_NAME_MISSING_FOR_CREATE = "Operation CREATE_KEY_FILE requires a keyFileName";

    @Option(name = "-t", aliases = {"--threads"}, usage = "number of threads")
    private int threads = 1;

    @Option(name = "-n", aliases = {"--number"}, usage = "number of operations", required = true)
    private int n = 1;

    @Option(name = "--size", usage = "file size (e.g. for UPLOAD); supported units: B, K, M", handler = IntFromByteUnitOptionHandler.class)
    private int size = 128 * 1024; // 128K

    @Option(name = "--accessKey", usage = "access key ID; also possible to set AWS_ACCESS_KEY int environment", required = true)
    private String accessKey = null;

    @Option(name = "--secretKey", usage = "secret access key; also possible to set AWS_SECRET_KEY in environment", required = true)
    private String secretKey = null;

    @Option(name = "--endpointUrl", usage = "endpoint url")
    private String endpointUrl = DEFAULT_S3_ENDPOINT;

    @Option(name = "--bucketName", usage = "name of bucket")
    private String bucketName = null;

    @Option(name = "--operation", usage = "operation")
    private String operation = Operation.UPLOAD.name();

    @Option(name = "--http", usage = "use http instead of https")
    private boolean useHttp = false;

    @Option(name = "--gzip", usage = "use gzip")
    private boolean useGzip = false;

    @Option(name = "--signerOverride", usage = "override the S3 signer (e.g. 'S3Signer' or 'AWSS3V4Signer')")
    private String signerOverride = null;

    @Option(name = "--keepAlive", usage = "use TCP keep alive")
    private boolean useKeepAlive = false;

    @Option(name = "--usePathStyleAccess", usage = "use path-style access (instead of DNS-style)")
    private boolean usePathStyleAccess = false;

    @Option(name = "--keyFileName", usage = "name of file with object keys")
    private String keyFileName = null;

    @Option(name = "--resultFileName", usage = "name of file with test results", hidden = true)
    private String resultFileName = null;

    private final List<String> commandLineArguments = new ArrayList<>();

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        new Main(args).run();
    }

    public Main(String... args) {
        commandLineArguments.addAll(Arrays.asList(args));
    }

    private void run() throws IOException {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            addArgumentsFromEnvironment("--accessKey", "AWS_ACCESS_KEY");
            addArgumentsFromEnvironment("--secretKey", "AWS_SECRET_KEY");

            parser.parseArgument(commandLineArguments);

            if (Operation.CREATE_KEY_FILE.name().equals(operation) && keyFileName == null) {
                throw new CmdLineException(parser, new IllegalStateException(KEY_FILE_NAME_MISSING_FOR_CREATE));
            }

        } catch (CmdLineException e) {

            System.err.println(e.getMessage());
            System.err.println("java -jar s3pt.jar [options...]");
            parser.printUsage(System.err);
            System.err.println();

            return;
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        TestResult testResult = new S3PerformanceTest(
                accessKey,
                secretKey,
                endpointUrl,
                bucketName,
                Operation.valueOf(operation),
                threads,
                n,
                size,
                useHttp,
                useGzip,
                signerOverride,
                useKeepAlive,
                usePathStyleAccess,
                keyFileName
        ).call();

        stopWatch.stop();
        LOG.info("Total time = {} ms", stopWatch.getTime());

        if (testResult != null) {
            testResult.log();
            if (resultFileName != null) {
                testResult.writeToFileAsJson(resultFileName);
            }
        }
    }

    private void addArgumentsFromEnvironment(String commandLineKey, String environmentKey) {
        String value = System.getenv(environmentKey);
        if (value != null) {
            boolean override = false;
            for (String arg : commandLineArguments) {
                if (commandLineKey.equals(arg)) {
                    LOG.info("Ignore environment value for {}. Use value supplied by {} (override).", environmentKey, commandLineKey);
                    override = true;
                    break;
                }
            }
            if (!override) {
                LOG.info("Use environment value for {}.", environmentKey);
                commandLineArguments.add(commandLineKey);
                commandLineArguments.add(value);
            }
        }
    }

}
