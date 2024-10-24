package ch.sdsc.zarr;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.$internal.guava.collect.ImmutableCollection;
import org.weakref.jmx.$internal.guava.collect.ImmutableList;

import java.util.List;

public class ZarrMetadata implements ConnectorMetadata {
    public ZarrMetadata() {
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of("default");
    }

    ImmutableCollection<String> listTables(ConnectorSession session, String schemaName) {
        return ImmutableList.of("default");
    }

}

