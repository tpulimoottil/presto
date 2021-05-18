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
package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkService;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkServiceFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfiguration;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import org.apache.spark.TaskContext;

import java.io.File;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

import static com.facebook.presto.spark.launcher.LauncherUtils.checkDirectory;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRunner
        implements AutoCloseable
{
    private final PrestoSparkDistribution distribution;
    private final IPrestoSparkService driverPrestoSparkService;

    public PrestoSparkRunner(PrestoSparkDistribution distribution)
    {
        this.distribution = requireNonNull(distribution, "distribution is null");
        driverPrestoSparkService = createService(
                SparkProcessType.DRIVER,
                distribution.getPackageSupplier(),
                distribution.getConfigProperties(),
                distribution.getCatalogProperties(),
                distribution.getEventListenerProperties(),
                distribution.getAccessControlProperties(),
                distribution.getSessionPropertyConfigurationProperties(),
                distribution.getFunctionNamespaceProperties());
    }

    public void run(
            String user,
            Optional<Principal> principal,
            Map<String, String> extraCredentials,
            String catalog,
            String schema,
            Optional<String> source,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Map<String, String> sessionProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Optional<String> traceToken,
            String query,
            Optional<String> sparkQueueName,
            Optional<Path> queryStatusInfoOutputPath,
            Optional<Path> queryDataOutputPath)
    {
        IPrestoSparkQueryExecutionFactory queryExecutionFactory = driverPrestoSparkService.getQueryExecutionFactory();

        PrestoSparkSession session = new PrestoSparkSession(
                user,
                principal,
                extraCredentials,
                Optional.ofNullable(catalog),
                Optional.ofNullable(schema),
                source,
                userAgent,
                clientInfo,
                clientTags,
                Optional.empty(),
                Optional.empty(),
                sessionProperties,
                catalogSessionProperties,
                traceToken);

        System.out.println("Query Execution Start 1");

        String tableName1 = "superglue_sandbox.thomas_presto_ctas_1days_" + System.currentTimeMillis();
        IPrestoSparkQueryExecution queryExecution = queryExecutionFactory.create(
                distribution.getSparkContext(),
                session,
                testQuery(tableName1).get(),
                sparkQueueName,
                new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(distribution),
                queryStatusInfoOutputPath,
                queryDataOutputPath);

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
        System.out.println("Query Execution Completed 1");

        String tableName2 = "superglue_sandbox.thomas_presto_ctas_1days_" + System.currentTimeMillis();
        IPrestoSparkQueryExecution queryExecution2 = queryExecutionFactory.create(
                distribution.getSparkContext(),
                session,
                testQuery(tableName2).get(),
                sparkQueueName,
                new DistributionBasedPrestoSparkTaskExecutorFactoryProvider(distribution),
                queryStatusInfoOutputPath,
                queryDataOutputPath);

        List<List<Object>> results2 = queryExecution.execute();

        System.out.println("Rows: " + results2.size());
        results.forEach(System.out::println);
        System.out.println("Query Execution Completed 2");

        System.out.println("Connecting to Hive");
        showTables("drop table " + tableName1);
        System.out.println("Completed Hive Query");

        System.out.println(tableName2 + " will not be deleted");
        System.out.println(tableName1 + " will get deleted");
    }
    public Optional<String> testQuery(String tableName)
    {
        String query = "create table if not exists " + tableName + " as" +
                " select" +
                "    c.pseudonym_id" +
                "  , max(c.event_ts) as event_ts" +
                "  , max(CASE WHEN c.ui_object_detail = 'businessprofile-tile-screen-multiSelect-0' THEN 1        ELSE 0        END) AS main_source" +
                "  ,        max(CASE WHEN c.ui_object_detail = 'businessprofile-tile-screen-multiSelect-1' THEN 1        ELSE 0 END) AS side_job" +
                "    , max(CASE WHEN c.ui_object_detail = 'businessprofile-tile-screen-multiSelect-2' THEN 1 ELSE 0 END) AS business" +
                "    , max(CASE WHEN c.ui_object_detail = 'businessprofile-tile-screen-multiSelect-3'THEN 1 ELSE 0        END) AS something_else" +
                "    , max(CASE WHEN c.ui_object_detail = 'businessprofile-startstop-views-0-actions-1'THEN 1 ELSE 0        END) AS click_continue" +
                "    , max(CASE WHEN screen IN ('businessprofile-tile-screen-view')THEN 1 ELSE 0        END) AS saw_tile_screen" +
                " from thrive_dwh.cg_turbotax_clickstream c" +
                " where c.screen in ('businessprofile-tile-screen-view')" +
                " and concat(YEAR,MONTH,DAY) >=" +
                "       date_format(" +
                "        current_timestamp - interval '1' day," +
                "           '%Y%m%d'" +
                "        )" +
                " and event_ts < current_timestamp" +
                " group by c.pseudonym_id, c.tax_prep_year";
        System.out.println("Query: " + query);
        return Optional.of(query);
    }
    public void showTables(String query)
    {
        Connection con = null;
        try {
            String conStr = "jdbc:spark://databricks.data-curation-prd.a.intuit.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/0/0904-214845-gizmo541;AuthMech=3;UID=token;PWD=dapidb7f838a81145ba5f3b67d4c48f85d98";
            Class.forName("com.simba.spark.jdbc41.Driver");
            con = DriverManager.getConnection(conStr);
            Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
//            while (showDatabases.next()) {
//                System.out.println(showDatabases.getString(1));
//            }
            //System.out.println("show database successfully.");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            try {
                if (con != null) {
                    con.close();
                }
            }
            catch (Exception ex) {
            }
        }
    }

    @Override
    public void close()
    {
        driverPrestoSparkService.close();
    }

    private static IPrestoSparkServiceFactory createServiceFactory(File directory)
    {
        checkDirectory(directory);
        List<URL> urls = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files != null) {
            sort(files);
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new UncheckedIOException(e);
            }
        }
        PrestoSparkLoader prestoSparkLoader = new PrestoSparkLoader(
                urls,
                PrestoSparkLauncher.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.classloader_interface.", "scala.", "com.facebook.di.security.token_service."));
        ServiceLoader<IPrestoSparkServiceFactory> serviceLoader = ServiceLoader.load(IPrestoSparkServiceFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    private static IPrestoSparkService createService(
            SparkProcessType sparkProcessType,
            PackageSupplier packageSupplier,
            Map<String, String> configProperties,
            Map<String, Map<String, String>> catalogProperties,
            Optional<Map<String, String>> eventListenerProperties,
            Optional<Map<String, String>> accessControlProperties,
            Optional<Map<String, String>> sessionPropertyConfigurationProperties,
            Optional<Map<String, Map<String, String>>> functionNamespaceProperties)
    {
        String packagePath = getPackagePath(packageSupplier);
        File pluginsDirectory = checkDirectory(new File(packagePath, "plugin"));
        PrestoSparkConfiguration configuration = new PrestoSparkConfiguration(
                configProperties,
                pluginsDirectory.getAbsolutePath(),
                catalogProperties,
                eventListenerProperties,
                accessControlProperties,
                sessionPropertyConfigurationProperties,
                functionNamespaceProperties);
        IPrestoSparkServiceFactory serviceFactory = createServiceFactory(checkDirectory(new File(packagePath, "lib")));
        return serviceFactory.createService(sparkProcessType, configuration);
    }

    private static String getPackagePath(PackageSupplier packageSupplier)
    {
        return checkDirectory(packageSupplier.getPrestoSparkPackageDirectory()).getAbsolutePath();
    }

    private static class DistributionBasedPrestoSparkTaskExecutorFactoryProvider
            implements PrestoSparkTaskExecutorFactoryProvider
    {
        private final PackageSupplier packageSupplier;
        private final Map<String, String> configProperties;
        private final Map<String, Map<String, String>> catalogProperties;
        private final Map<String, String> eventListenerProperties;
        private final Map<String, String> accessControlProperties;
        private final Map<String, String> sessionPropertyConfigurationProperties;
        private final Map<String, Map<String, String>> functionNamespaceProperties;

        public DistributionBasedPrestoSparkTaskExecutorFactoryProvider(PrestoSparkDistribution distribution)
        {
            requireNonNull(distribution, "distribution is null");
            this.packageSupplier = distribution.getPackageSupplier();
            this.configProperties = distribution.getConfigProperties();
            this.catalogProperties = distribution.getCatalogProperties();
            // Optional is not Serializable
            this.eventListenerProperties = distribution.getEventListenerProperties().orElse(null);
            this.accessControlProperties = distribution.getAccessControlProperties().orElse(null);
            this.sessionPropertyConfigurationProperties = distribution.getSessionPropertyConfigurationProperties().orElse(null);
            this.functionNamespaceProperties = distribution.getFunctionNamespaceProperties().orElse(null);
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            checkState(TaskContext.get() != null, "this method is expected to be called only from the main task thread on the spark executor");
            IPrestoSparkService prestoSparkService = getOrCreatePrestoSparkService();
            return prestoSparkService.getTaskExecutorFactory();
        }

        private static IPrestoSparkService service;
        private static String currentPackagePath;
        private static Map<String, String> currentConfigProperties;
        private static Map<String, Map<String, String>> currentCatalogProperties;
        private static Map<String, String> currentEventListenerProperties;
        private static Map<String, String> currentAccessControlProperties;
        private static Map<String, String> currentSessionPropertyConfigurationProperties;
        private static Map<String, Map<String, String>> currentFunctionNamespaceProperties;

        private IPrestoSparkService getOrCreatePrestoSparkService()
        {
            synchronized (DistributionBasedPrestoSparkTaskExecutorFactoryProvider.class) {
                if (service == null) {
                    service = createService(
                            SparkProcessType.EXECUTOR,
                            packageSupplier,
                            configProperties,
                            catalogProperties,
                            Optional.ofNullable(eventListenerProperties),
                            Optional.ofNullable(accessControlProperties),
                            Optional.ofNullable(sessionPropertyConfigurationProperties),
                            Optional.ofNullable(functionNamespaceProperties));

                    currentPackagePath = getPackagePath(packageSupplier);
                    currentConfigProperties = configProperties;
                    currentCatalogProperties = catalogProperties;
                    currentEventListenerProperties = eventListenerProperties;
                    currentAccessControlProperties = accessControlProperties;
                    currentSessionPropertyConfigurationProperties = sessionPropertyConfigurationProperties;
                    currentFunctionNamespaceProperties = functionNamespaceProperties;
                }
                else {
                    checkEquals("packagePath", currentPackagePath, getPackagePath(packageSupplier));
                    checkEquals("configProperties", currentConfigProperties, configProperties);
                    checkEquals("catalogProperties", currentCatalogProperties, catalogProperties);
                    checkEquals("eventListenerProperties", currentEventListenerProperties, eventListenerProperties);
                    checkEquals("accessControlProperties", currentAccessControlProperties, accessControlProperties);
                    checkEquals("sessionPropertyConfigurationProperties",
                            currentSessionPropertyConfigurationProperties,
                            sessionPropertyConfigurationProperties);
                    checkEquals("functionNamespaceProperties", currentFunctionNamespaceProperties, functionNamespaceProperties);
                }
                return service;
            }
        }

        public static void checkEquals(String name, Object first, Object second)
        {
            if (!Objects.equals(first, second)) {
                throw new IllegalStateException(format("%s is different: %s != %s", name, first, second));
            }
        }
    }
}
