package ch.sdsc.zarr;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;

public class ZarrConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "zarr";
    }
    public static Connector createConnector() {
        return new ZarrConnector();
    }
}
