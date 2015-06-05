package de.jeha.s3pt;

import com.amazonaws.auth.SignerFactory;
import com.amazonaws.services.s3.internal.S3Signer;
import org.apache.commons.lang3.time.StopWatch;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * @author jenshadlich@googlemail.com
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    @Option(name = "-t", usage = "number of threads")
    private int threads = 1;

    @Option(name = "-n", usage = "number of operations", required = true)
    private int n;

    @Option(name = "--size", usage = "number of files")
    private int size = 128 * 1024; // 128 kb

    @Option(name = "--accessKey", usage = "access key ID", required = true)
    private String accessKey;

    @Option(name = "--secretKey", usage = "secret access key", required = true)
    private String secretKey;

    @Option(name = "--endpointUrl", usage = "endpoint url")
    private String endpointUrl = "s3.amazonaws.com";

    @Option(name = "--bucketName", usage = "name of bucket")
    private String bucketName;

    @Option(name = "--operation", usage = "operation")
    private String operation = Operation.UPLOAD.name();

    @Option(name = "--http", usage = "use http instead of https")
    private boolean useHttp = false;

    @Option(name = "--gzip", usage = "use gzip")
    private boolean useGzip = false;

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        // register "old" signer because Ceph / radosgw does not support SigV4 signing
        SignerFactory.registerSigner("S3Signer", S3Signer.class);

        new Main().run(args);
    }

    private void run(String... args) throws IOException {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);

        } catch (CmdLineException e) {

            System.err.println(e.getMessage());
            System.err.println("java -jar s3pt.jar [options...]");
            parser.printUsage(System.err);
            System.err.println();

            return;
        }

        StopWatch stopWatch = new StopWatch();

        stopWatch.start();

        new S3PerformanceTest(
                accessKey,
                secretKey,
                endpointUrl,
                bucketName,
                Operation.valueOf(operation),
                threads,
                n,
                size,
                useHttp,
                useGzip
        ).run();

        stopWatch.stop();

        LOG.info("Total time = {} ms", stopWatch.getTime());
    }

}
