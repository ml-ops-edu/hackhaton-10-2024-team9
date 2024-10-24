package ch.sdsc.zarr;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class ZarrConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "zarr";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new ZarrModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ZarrConnector.class);
    }

    public static Connector createConnector() {
        return new ZarrConnector();
    }
}
