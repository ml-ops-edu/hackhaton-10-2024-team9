package ch.sdsc.zarr;
import com.google.inject.Binder;
import com.google.inject.Module;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ZarrModule  implements  Module{
    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(ZarrConfig.class);
    }
    public ZarrModule() {
    }
}
