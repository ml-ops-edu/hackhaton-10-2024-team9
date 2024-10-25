/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.sdsc.zarr;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationLoader;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;
import com.google.inject.Inject; // NEW


public class ZarrQueryRunner
{
    private ZarrConfig zarrConfig;


    @Inject // NEW
    public ZarrQueryRunner(ZarrConfig zarrConfig) {
        this.zarrConfig = zarrConfig;
    }
    public static QueryRunner createQueryRunner()
            throws Exception
    {
        Logger log = Logger.get(ZarrQueryRunner.class);
        log.info("======== SERVER STARTING ========");
        Session defaultSession = testSessionBuilder()
                .setCatalog("zar-connector")
                .setSchema("default")
                .build();

        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8080"))
                .build();
        System.out.println(extraProperties);
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(extraProperties)
                .setNodeCount(1)
                .build();
        System.out.println(queryRunner);
        queryRunner.installPlugin(new ZarrPlugin());



        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("ch.sdsc.zarr", Level.DEBUG);
        logger.setLevel("io.trino", Level.DEBUG);

        try (QueryRunner queryRunner = createQueryRunner()) {

            Logger log = Logger.get(ZarrQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            String baseUrl = ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl().toString();
            log.info("\n====\n%s\n====", baseUrl.replace("localhost/127.0.0.1", "localhost"));
            //log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
        }
    }
}