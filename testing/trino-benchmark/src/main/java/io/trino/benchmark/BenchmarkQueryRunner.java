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
package io.trino.benchmark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Module;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.hive.HiveHandleResolver;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.testing.LocalQueryRunner;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.Session.SessionBuilder;
import static io.trino.plugin.hive.InternalHiveConnectorFactory.createConnector;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class BenchmarkQueryRunner {
    private BenchmarkQueryRunner() {
    }

    public static LocalQueryRunner createLocalQueryRunnerHashEnabled() {
        return createLocalQueryRunner(ImmutableMap.of("optimizer.optimize_hash_generation", "true"));
    }

    public static class TestingHiveConnectorFactory
            implements ConnectorFactory {
        private final HiveMetastore metastore;
        private final Module module;

        public TestingHiveConnectorFactory(HiveMetastore metastore) {
            this(metastore, EMPTY_MODULE);
        }

        public TestingHiveConnectorFactory(HiveMetastore metastore, Module module) {
            this.metastore = requireNonNull(metastore, "metastore is null");
            this.module = requireNonNull(module, "module is null");
        }

        @Override
        public String getName() {
            return "hive";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver() {
            return new HiveHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
            return createConnector(catalogName, config, context, module, Optional.of(metastore));
        }
    }

    static PrincipalPrivileges testingPrincipalPrivilege(String tableOwner, String grantor) {
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.INSERT, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.UPDATE, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .put(tableOwner, new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.DELETE, true, new HivePrincipal(USER, grantor), new HivePrincipal(USER, grantor)))
                        .build(),
                ImmutableMultimap.of());
    }

    public static LocalQueryRunner createLocalQueryRunner() {
        File tempDir = new File("/workspace/tpch-data");
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();


        Session session = testSessionBuilder(sessionPropertyManager)
                .setCatalog("hive")
                .setSchema("tpch")
                .setCatalogSessionProperty("hive", "parquet_use_column_names", "true")
                .build();

        LocalQueryRunner localQueryRunner = LocalQueryRunner.create(session);

//         add tpch
//        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        // add hive
        File hiveDir = new File(tempDir, "hive_data");
        HiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);

        HiveIdentity identity = new HiveIdentity(SESSION);
//        metastore.createDatabase(identity,
//                Database.builder()
//                        .setDatabaseName("tpch")
//                        .setOwnerName("public")
//                        .setOwnerType(PrincipalType.ROLE)
//                        .build());

        Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
                .put("hive.max-split-size", "10GB")
                .build();

        localQueryRunner.createCatalog("hive", new TestingHiveConnectorFactory(metastore), hiveCatalogConfig);

//        localQueryRunner.execute("CREATE TABLE orders WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.orders");
//        localQueryRunner.execute("CREATE TABLE lineitem WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.lineitem");
//        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
//        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        return localQueryRunner;
    }

    public static LocalQueryRunner createLocalQueryRunner(Map<String, String> extraSessionProperties) {
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);

        extraSessionProperties.forEach(sessionBuilder::setSystemProperty);

        Session session = sessionBuilder.build();
        LocalQueryRunner localQueryRunner = LocalQueryRunner.create(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        return localQueryRunner;
    }
}
