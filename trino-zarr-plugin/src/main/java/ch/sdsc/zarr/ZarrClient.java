package ch.sdsc.zarr;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.bc.zarr.ZarrGroup;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Date;
import java.util.Set;

sealed interface S3AuthConfig permits AnonymousS3Config, AuthenticatedS3Config {
    static S3AuthConfig fromZarrConfig(ZarrConfig config) {
        if (config.getAnonymous()) {
            return new AnonymousS3Config();
        } else {
            return new AuthenticatedS3Config(config.getAccessKey(), config.getSecretKey());
        }
    }
}

record AnonymousS3Config() implements S3AuthConfig {
}

record AuthenticatedS3Config(String accessKey, String secretKey) implements S3AuthConfig {
}


sealed interface S3EndpointConfig permits AWSEndPoint, CustomEndpoint {
    static S3EndpointConfig fromZarrConfig(ZarrConfig config) {
        if (config.getAws()) {
            return new AWSEndPoint(config.getRegion());
        } else {
            return new CustomEndpoint(config.getUrl());
        }
    }
}

record AWSEndPoint(String region) implements S3EndpointConfig {
}

record CustomEndpoint(String url) implements S3EndpointConfig {
}

record S3Config(S3AuthConfig authConfig, S3EndpointConfig endpointConfig) {
}


public class ZarrClient {
    private AmazonS3 s3Client;
    private ZarrGroup zarrGroup;


    private static AmazonS3ClientBuilder getBuilder(Boolean aws)

    private static AmazonS3 createClient(ZarrConfig config) {
        var authConfig = S3AuthConfig.fromZarrConfig(config);
        var endpointConfig = S3EndpointConfig.fromZarrConfig(config);

    }

    private GetObjectRequest createGetObjectRequest(ZarrConfig config, String key) {
        return new GetObjectRequest(config.getBucket(), key);
    }

    private static Date generateExpirationDate() {
        Date expiration = new Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60;  // URL valid for 1 hour
        expiration.setTime(expTimeMillis);
        return expiration;
    }

    public ZarrClient(ZarrConfig config) throws IOException {

        s3Client = createClient(config);
        var request = createGetObjectRequest(config, config.getKey());
        url = s3Client.generatePresignedUrl(config.getBucket(), config.getKey(), generateExpirationDate());

    }

    private void openZarrGroup() throws IOException {
        FileSystem s3fs = FileSystems.newFileSystem(URI.create(url.toString()), null);
        zarrGroup = ZarrGroup.open(s3fs.getPath("/"));
    }


    public Set<String> getTableNames() {
        try {
            openZarrGroup();
            return zarrGroup.getArrayKeys();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
