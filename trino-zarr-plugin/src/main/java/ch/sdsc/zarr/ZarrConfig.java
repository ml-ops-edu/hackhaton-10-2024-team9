package ch.sdsc.zarr;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;



public class ZarrConfig {
    private String accessKey;
    private String url;
    private String bucket;
    private String key;
    private String secretKey;

    public String getRegion() {
        return region;
    }

    @Config("region")
    @ConfigDescription("Region of the data source")
    public ZarrConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    @Config("aws")
    @ConfigDescription("Whether the data source is on AWS")
    public Boolean getAws() {
        return aws;
    }

    public ZarrConfig setAws(Boolean aws) {
        this.aws = aws;
        return this;
    }

    private String region;
    private Boolean aws;

    public Boolean getAnonymous() {
        return anonymous;
    }

    @Config("anonymous")
    @ConfigDescription("Whether the data source is anonymous")
    public ZarrConfig setAnonymous(Boolean anonymous) {
        this.anonymous = anonymous;
        return this;
    }

    private Boolean anonymous = false;


    public String getUrl() {
        return url;
    }

    @Config("url")
    @ConfigDescription("URL to the S3 bucket")
    public ZarrConfig setUrl(String url) {
        this.url = url;
        return this;
    }


    public String getAccessKey() {
        return accessKey;
    }

    @Config("accessKey")
    @ConfigDescription("Access key required to access the data source")
    public ZarrConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }



    public String getSecretKey() {
        return secretKey;
    }

    @Config("secretKey")
    @ConfigDescription("Secret required to access the data source")
    @ConfigSecuritySensitive
    public ZarrConfig setSecretKey(String secret) {
        this.secretKey = secret;
        return this;
    }


    public String getBucket() {
        return bucket;
    }

    public ZarrConfig setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public String getKey() {
        return key;
    }

    public ZarrConfig setKey(String key) {
        this.key = key;
        return this;
    }
}
