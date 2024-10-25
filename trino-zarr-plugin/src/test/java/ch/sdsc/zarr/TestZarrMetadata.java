package ch.sdsc.zarr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestZarrMetadata {
    private ZarrClient client;

    @BeforeEach
    public void setUp()
            throws Exception {
        client = new ZarrClient(new ZarrConfig().setAws(true).setBucket("hrrrzarr").setRegion("us-west-1").setAnonymous(true).setAccessKey(".zgroup"));

    }

    @Test
    public void testListSchemaNames() {
        client.getTableNames();
    }
}
