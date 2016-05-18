package de.jeha.s3pt.tools;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import de.jeha.s3pt.Constants;
import de.jeha.s3pt.utils.UserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * @author jenshadlich@googlemail.com
 */
public class MultiPartUpload {

    private static final Logger LOG = LoggerFactory.getLogger(MultiPartUpload.class);

    public static void main(String... args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        final Properties userProperties = UserProperties.fromHome().load("s3pt");
        final String accessKey = userProperties.getProperty("accessKey");
        final String secretKey = userProperties.getProperty("secretKey");
        final String endpoint = userProperties.getProperty("endpoint");
        final String bucket = "testbucket";
        final String key = "multipart_test.svg";

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withUserAgent("MultiPartUpload")
                .withSignerOverride(Constants.S3_SIGNER_TYPE);

        AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfiguration);
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).disableChunkedEncoding().build());
        s3Client.setEndpoint(endpoint);

        List<PartETag> partETags = new ArrayList<>();

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        File file = new File("/User/jns/flamegraph.svg");
        long contentLength = file.length();
        long partSize = 32 * 1024;

        try {
            // Step 2: Upload parts.
            long filePosition = 0;
            for (int i = 1; filePosition < contentLength; i++) {
                // Last part can be smaller. Adjust part size.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucket).withKey(key)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload part and add response to our list.
                partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

                filePosition += partSize;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(bucket,
                    key,
                    initResponse.getUploadId(),
                    partETags);

            s3Client.completeMultipartUpload(compRequest);
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
                    bucket, key, initResponse.getUploadId()));
        }

    }

}
