package ch.sdsc.zarr;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.$internal.guava.collect.ImmutableList;

public class ZarrPlugin implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ZarrConnectorFactory());
    }
}
