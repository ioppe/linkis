/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.engineconnplugin.flink.client.context;

import org.apache.linkis.engineconnplugin.flink.client.config.Environment;
import org.apache.linkis.engineconnplugin.flink.client.factory.LinkisKubernetesClusterClientFactory;
import org.apache.linkis.engineconnplugin.flink.client.factory.LinkisYarnClusterClientFactory;
import org.apache.linkis.engineconnplugin.flink.exception.SqlExecutionException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.resource.ClientResourceManager;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.linkis.engineconnplugin.flink.errorcode.FlinkErrorCodeSummary.*;

public class ExecutionContext {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

  private final Environment environment;
  private final ClassLoader classLoader;

  private final Configuration flinkConfig;
  private LinkisYarnClusterClientFactory clusterClientFactory;

  private LinkisKubernetesClusterClientFactory kubernetesClusterClientFactory;

  private TableEnvironment tableEnv;
  private ExecutionEnvironment execEnv;
  private StreamExecutionEnvironment streamExecEnv;
  private Executor executor;

  // Members that should be reused in the same session.
  private SessionState sessionState;

  private ExecutionContext(
      Environment environment,
      @Nullable SessionState sessionState,
      List<URL> dependencies,
      Configuration flinkConfig) {
    this(
        environment,
        sessionState,
        dependencies,
        flinkConfig,
        new LinkisYarnClusterClientFactory(),
        new LinkisKubernetesClusterClientFactory());
  }

  private ExecutionContext(
      Environment environment,
      @Nullable SessionState sessionState,
      List<URL> dependencies,
      Configuration flinkConfig,
      LinkisYarnClusterClientFactory clusterClientFactory) {
    this(
        environment,
        sessionState,
        dependencies,
        flinkConfig,
        clusterClientFactory,
        new LinkisKubernetesClusterClientFactory());
  }

  private ExecutionContext(
      Environment environment,
      @Nullable SessionState sessionState,
      List<URL> dependencies,
      Configuration flinkConfig,
      LinkisYarnClusterClientFactory clusterClientFactory,
      LinkisKubernetesClusterClientFactory linkisKubernetesClusterClientFactory) {
    this.environment = environment;
    this.flinkConfig = flinkConfig;
    this.sessionState = sessionState;
    // create class loader
    if (dependencies == null) {
      dependencies = Collections.emptyList();
    }
    classLoader =
        ClientUtils.buildUserCodeClassLoader(
            dependencies, Collections.emptyList(), this.getClass().getClassLoader(), flinkConfig);
    LOG.debug("Deployment descriptor: {}", environment.getDeployment());
    LOG.info("flinkConfig config: {}", flinkConfig);
    this.clusterClientFactory = clusterClientFactory;
    this.kubernetesClusterClientFactory = linkisKubernetesClusterClientFactory;
  }


  public StreamExecutionEnvironment getStreamExecutionEnvironment() throws SqlExecutionException {
    if (streamExecEnv == null) {
      getTableEnvironment();
    }
    return streamExecEnv;
  }

  public void setString(String key, String value) {
    this.flinkConfig.setString(key, value);
  }

  public void setBoolean(String key, boolean value) {
    this.flinkConfig.setBoolean(key, value);
  }

  public Configuration getFlinkConfig() {
    return flinkConfig;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public YarnClusterDescriptor createClusterDescriptor() {
    return clusterClientFactory.createClusterDescriptor(this.flinkConfig);
  }

  public KubernetesClusterDescriptor createKubernetesClusterDescriptor() {
    return kubernetesClusterClientFactory.createClusterDescriptor(this.flinkConfig);
  }

  public Map<String, Catalog> getCatalogs() {
    Map<String, Catalog> catalogs = new HashMap<>();
    for (String name : tableEnv.listCatalogs()) {
      tableEnv.getCatalog(name).ifPresent(c -> catalogs.put(name, c));
    }
    return catalogs;
  }

  public SessionState getSessionState() {
    return this.sessionState;
  }

  /**
   * Executes the given supplier using the execution context's classloader as thread classloader.
   */
  public <R> R wrapClassLoader(Supplier<R> supplier) {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
      return supplier.get();
    }
  }

  public <R> R wrapClassLoader(Function<TableEnvironmentInternal, R> function)
      throws SqlExecutionException {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
      return function.apply((TableEnvironmentInternal) getTableEnvironment());
    }
  }

  /**
   * Executes the given Runnable using the execution context's classloader as thread classloader.
   */
  void wrapClassLoader(Runnable runnable) {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
      runnable.run();
    }
  }

  public TableEnvironment getTableEnvironment() throws SqlExecutionException {
    if (tableEnv == null) {
      synchronized (this) {
        if (tableEnv == null) {
          // Initialize the TableEnvironment.
          this.streamExecEnv =
                  new StreamExecutionEnvironment(new Configuration(flinkConfig), classLoader);
          this.tableEnv =
                  (TableEnvironment)
                          this.createTableEnvironment(
                                  flinkConfig, streamExecEnv, sessionState, classLoader);
        }
      }
    }
    return tableEnv;
  }

  public ExecutionConfig getExecutionConfig() {
    if (streamExecEnv != null) {
      return streamExecEnv.getConfig();
    } else {
      return execEnv.getConfig();
    }
  }

  public LinkisYarnClusterClientFactory getClusterClientFactory() {
    return clusterClientFactory;
  }

  /** Returns a builder for this {@link ExecutionContext}. */
  public static Builder builder(
      Environment defaultEnv,
      Environment sessionEnv,
      List<URL> dependencies,
      Configuration configuration,
      String flinkVersion) {
    return new Builder(defaultEnv, sessionEnv, dependencies, configuration, flinkVersion);
  }

  public static ExecutionContext getInstance(String flinkVersion) throws Exception {
    Class<?> flinkShimsClass;
    flinkShimsClass = Class.forName("org.apache.linkis.engineconnplugin.flink.client.context.ExecutionContext");
    Constructor c =flinkShimsClass.getConstructor(String.class);
    return (ExecutionContext) c.newInstance(flinkVersion);
  }

  public CompletableFuture<String> triggerSavepoint(
          Object clusterClientObject, Object jobIdObject, String savepoint) {
    ClusterClient clusterClient = (ClusterClient) clusterClientObject;
    return clusterClient.triggerSavepoint(
            (JobID) jobIdObject, savepoint, SavepointFormatType.CANONICAL);
  }


  public CompletableFuture<String> cancelWithSavepoint(
          Object clusterClientObject, Object jobIdObject, String savepoint) {
    ClusterClient clusterClient = (ClusterClient) clusterClientObject;
    return clusterClient.cancelWithSavepoint(
            (JobID) jobIdObject, savepoint, SavepointFormatType.CANONICAL);
  }


  public CompletableFuture<String> stopWithSavepoint(
          Object clusterClientObject,
          Object jobIdObject,
          boolean advanceToEndOfEventTime,
          String savepoint) {
    ClusterClient clusterClient = (ClusterClient) clusterClientObject;
    return clusterClient.stopWithSavepoint(
            (JobID) jobIdObject, advanceToEndOfEventTime, savepoint, SavepointFormatType.CANONICAL);
  }


  private static StreamTableEnvironment createStreamTableEnvironment(
      StreamExecutionEnvironment env,
      EnvironmentSettings settings,
      Executor executor,
      CatalogManager catalogManager,
      ModuleManager moduleManager,
      ResourceManager resourceManager,
      FunctionCatalog functionCatalog,
      ClassLoader userClassLoader) {
//    final Map<String, String> plannerProperties = settings.toPlannerProperties();
    TableConfig tableConfig = TableConfig.getDefault();
    tableConfig.setRootConfiguration(executor.getConfiguration());
    tableConfig.addConfiguration(settings.getConfiguration());
//    final Planner planner =
//        ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
//            .create(plannerProperties, executor, config, functionCatalog, catalogManager);
    final Planner planner =
            PlannerFactoryUtil.createPlanner(
                    executor, tableConfig, userClassLoader, moduleManager, catalogManager, functionCatalog);

    return new StreamTableEnvironmentImpl(
        catalogManager,
        moduleManager,
        resourceManager,
        functionCatalog,
        tableConfig,
        env,
        planner,
        executor,
        settings.isStreamingMode());
  }

  private static Executor lookupExecutor(
          StreamExecutionEnvironment executionEnvironment, ClassLoader userClassLoader) {
    try {
      final ExecutorFactory executorFactory =
              FactoryUtil.discoverFactory(
                      userClassLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
      final Method createMethod =
              executorFactory.getClass().getMethod("create", StreamExecutionEnvironment.class);

      return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
    } catch (Exception e) {
      throw new TableException(
              "Could not instantiate the executor. Make sure a planner module is on the classpath", e);
    }
  }

  public Object createTableEnvironment(
          Object flinkConfig, Object streamExecEnv, Object sessionState, ClassLoader classLoader) {
    Configuration flinkConfigConfiguration = (Configuration) flinkConfig;
    SessionState sessionStateByFlink = (SessionState) sessionState;
    if (sessionStateByFlink == null) {
      MutableURLClassLoader mutableURLClassLoader =
              FlinkUserCodeClassLoaders.create(new URL[0], classLoader, flinkConfigConfiguration);
      final ClientResourceManager resourceManager =
              new ClientResourceManager(flinkConfigConfiguration, mutableURLClassLoader);

      final ModuleManager moduleManager = new ModuleManager();

      final EnvironmentSettings settings =
              EnvironmentSettings.newInstance().withConfiguration(flinkConfigConfiguration).build();

      final CatalogManager catalogManager =
              CatalogManager.newBuilder()
                      .classLoader(classLoader)
                      .config(flinkConfigConfiguration)
                      .defaultCatalog(
                              settings.getBuiltInCatalogName(),
                              new GenericInMemoryCatalog(
                                      settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()))
                      .build();

      final FunctionCatalog functionCatalog =
              new FunctionCatalog(
                      flinkConfigConfiguration, resourceManager, catalogManager, moduleManager);
      sessionStateByFlink =
              new SessionState(catalogManager, moduleManager, resourceManager, functionCatalog);
    }

    EnvironmentSettings settings =
            EnvironmentSettings.newInstance().withConfiguration(flinkConfigConfiguration).build();

    if (streamExecEnv == null) {
      streamExecEnv =
              new StreamExecutionEnvironment(new Configuration(flinkConfigConfiguration), classLoader);
    }

    final Executor executor =
            lookupExecutor((StreamExecutionEnvironment) streamExecEnv, classLoader);

    return createStreamTableEnvironment(
            (StreamExecutionEnvironment) streamExecEnv,
            settings,
            executor,
            sessionStateByFlink.catalogManager,
            sessionStateByFlink.moduleManager,
            sessionStateByFlink.resourceManager,
            sessionStateByFlink.functionCatalog,
            classLoader);
  }


  private StreamExecutionEnvironment createStreamExecutionEnvironment() {
    StreamContextEnvironment.setAsContext(
        new DefaultExecutorServiceLoader(), flinkConfig, classLoader, false, false);
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
    env.setRestartStrategy(environment.getExecution().getRestartStrategy());
    env.setParallelism(environment.getExecution().getParallelism());
    env.setMaxParallelism(environment.getExecution().getMaxParallelism());
    env.setStreamTimeCharacteristic(environment.getExecution().getTimeCharacteristic());
    if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
      env.getConfig()
          .setAutoWatermarkInterval(environment.getExecution().getPeriodicWatermarksInterval());
    }
    return env;
  }

  public ExecutionContext cloneExecutionContext(Builder builder) {
    ExecutionContext newExecutionContext =
        builder.clusterClientFactory(clusterClientFactory).build();
    if (this.tableEnv != null) {
      newExecutionContext.tableEnv = tableEnv;
      newExecutionContext.execEnv = execEnv;
      newExecutionContext.streamExecEnv = streamExecEnv;
      newExecutionContext.executor = executor;
    }
    return newExecutionContext;
  }

  // ~ Inner Class -------------------------------------------------------------------------------

  /** Builder for {@link ExecutionContext}. */
  public static class Builder {
    // Required members.
    private final Environment sessionEnv;
    private final List<URL> dependencies;
    private final Configuration configuration;
    private Environment defaultEnv;
    private Environment currentEnv;
    private String flinkVersion;

    private LinkisYarnClusterClientFactory clusterClientFactory;

    // Optional members.
    @Nullable private SessionState sessionState;

    private Builder(
        Environment defaultEnv,
        @Nullable Environment sessionEnv,
        List<URL> dependencies,
        Configuration configuration,
        String flinkVersion) {
      this.defaultEnv = defaultEnv;
      this.sessionEnv = sessionEnv;
      this.dependencies = dependencies;
      this.configuration = configuration;
      this.flinkVersion = flinkVersion;
    }

    public Builder env(Environment environment) {
      this.currentEnv = environment;
      return this;
    }

    public Builder sessionState(SessionState sessionState) {
      this.sessionState = sessionState;
      return this;
    }

    Builder clusterClientFactory(LinkisYarnClusterClientFactory clusterClientFactory) {
      this.clusterClientFactory = clusterClientFactory;
      return this;
    }

    public ExecutionContext build() {
      if (sessionEnv == null) {
        this.currentEnv = defaultEnv;
      }
      if (clusterClientFactory == null) {
        return new ExecutionContext(
            this.currentEnv == null ? Environment.merge(defaultEnv, sessionEnv) : this.currentEnv,
            this.sessionState,
            this.dependencies,
            this.configuration);
      } else {
        return new ExecutionContext(
            this.currentEnv == null ? Environment.merge(defaultEnv, sessionEnv) : this.currentEnv,
            this.sessionState,
            this.dependencies,
            this.configuration,
            this.clusterClientFactory);
      }
    }
  }

  /** Represents the state that should be reused in one session. * */
  public static class SessionState {
    public final CatalogManager catalogManager;
    public final ModuleManager moduleManager;
    public final ClientResourceManager resourceManager;
    public final FunctionCatalog functionCatalog;

    private SessionState(
        CatalogManager catalogManager,
        ModuleManager moduleManager,
        ClientResourceManager resourceManager,
        FunctionCatalog functionCatalog) {
      this.catalogManager = catalogManager;
      this.moduleManager = moduleManager;
      this.resourceManager = resourceManager;
      this.functionCatalog = functionCatalog;
    }

    public static SessionState of(
        CatalogManager catalogManager,
        ModuleManager moduleManager,
        ClientResourceManager resourceManager,
        FunctionCatalog functionCatalog) {
      return new SessionState(catalogManager, moduleManager, resourceManager, functionCatalog);
    }
  }
}
