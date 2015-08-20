package de.jeha.s3pt.tools;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.S3Signer;
import com.amazonaws.services.s3.model.ObjectListing;
import de.jeha.s3pt.Constants;
import de.jeha.s3pt.utils.UserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * @author jenshadlich@googlemail.com
 */
public class CountObjects {

    private static final Logger LOG = LoggerFactory.getLogger(CountObjects.class);

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        Properties userProperties = UserProperties.read("s3pt");

        final String accessKey = userProperties.getProperty("accessKey");
        final String secretKey = userProperties.getProperty("secretKey");
        final String endpoint = userProperties.getProperty("endpoint");
        final String bucket = "my-bucket";

        SignerFactory.registerSigner(Constants.S3_SIGNER, S3Signer.class);

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withUserAgent("CountObjects")
                .withSignerOverride(Constants.S3_SIGNER);

        AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfig);
        s3Client.setEndpoint(endpoint);

        int objectCount = 0;
        int chunks = 0;

        LOG.info("Start: bucket '{}'", bucket);

        boolean truncated;
        ObjectListing previousObjectListing = null;
        do {
            chunks++;
            System.out.print(".");
            System.out.flush();
            if (chunks % 100 == 0) {
                System.out.println();
            }

            ObjectListing objectListing = (previousObjectListing != null)
                    ? s3Client.listNextBatchOfObjects(previousObjectListing)
                    : s3Client.listObjects(bucket);
            previousObjectListing = objectListing;
            truncated = objectListing.isTruncated();

            objectCount += objectListing.getObjectSummaries().size();
        } while (truncated);

        System.out.println();
        LOG.info("#objects: {}", objectCount);
    }

}
