package ch.sdsc.zarr;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;


public class ZarrConfig {

    public String getUrl() {
        return url;
    }

    @Config("url")
    @ConfigDescription("URL to the S3 bucket")
    public ZarrConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    private String key;
    private String url;

    public String getKey()
    {
        return key;
    }

    @Config("key")
    @ConfigDescription("Secret required to access the data source")
    @ConfigSecuritySensitive
    public ZarrConfig setKey(String secret)
    {
        this.key = secret;
        return this;
    }


}
