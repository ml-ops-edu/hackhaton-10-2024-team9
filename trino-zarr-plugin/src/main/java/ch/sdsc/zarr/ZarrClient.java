package ch.sdsc.zarr;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
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

    static AWSStaticCredentialsProvider getCredentialsProvider(S3AuthConfig config) {
        if (config instanceof AnonymousS3Config) {
            return new AWSStaticCredentialsProvider(new AnonymousAWSCredentials());
        } else {
            var authConfig = (AuthenticatedS3Config) config;
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(authConfig.accessKey(), authConfig.secretKey()));
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

    static AmazonS3ClientBuilder getBuilder(S3EndpointConfig config) {
        if (config instanceof AWSEndPoint) {
            return AmazonS3ClientBuilder.standard().withRegion(((AWSEndPoint) config).region());
        } else {
            return AmazonS3ClientBuilder.standard().withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(((CustomEndpoint) config).url(), "us-west-2"));
        }
    }
}

record AWSEndPoint(String region) implements S3EndpointConfig {
}

record CustomEndpoint(String url) implements S3EndpointConfig {
}

record S3Config(S3AuthConfig authConfig, S3EndpointConfig endpointConfig) {
    static S3Config fromZarrConfig(ZarrConfig config) {
        return new S3Config(S3AuthConfig.fromZarrConfig(config), S3EndpointConfig.fromZarrConfig(config));
    }
}


public class ZarrClient {
    private AmazonS3 s3Client;
    private ZarrGroup zarrGroup;
    private final URI url;


    private static AmazonS3 createClient(ZarrConfig config) {
        var s3Config = S3Config.fromZarrConfig(config);
        var builder = S3EndpointConfig.getBuilder(s3Config.endpointConfig());
        builder.withCredentials(S3AuthConfig.getCredentialsProvider(s3Config.authConfig()));
        return builder.build();


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
        url = URI.create(s3Client.generatePresignedUrl(config.getBucket(), config.getKey(), generateExpirationDate()).toString());

    }

    private void openZarrGroup() throws IOException {
        FileSystem s3fs = FileSystems.newFileSystem(URI.create(url.toString()), null);
        zarrGroup = ZarrGroup.open(s3fs.getPath(""));
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
