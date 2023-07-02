/*-
 *  § 
 * benchmark: command-line
 *    
 * Copyright (C) 2019 OnGres, Inc.
 *    
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * § §
 */

package com.ongres.benchmark;

import com.codahale.metrics.MetricFilter;
import com.google.common.io.Closer;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.ongres.benchmark.App.InitialValueAsDefaultValueProvider;
import com.ongres.benchmark.config.ConfigUtils;
import com.ongres.benchmark.config.Version;
import com.ongres.benchmark.config.model.Config;
import com.ongres.benchmark.jdbc.ConnectionSupplier;
import com.ongres.benchmark.jdbc.HikariConnectionSupplier;
import com.ongres.benchmark.jdbc.PostgresConnectionSupplier;
import com.zaxxer.hikari.HikariConfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.jooq.lambda.Unchecked;
import org.postgresql.PGProperty;
import org.reactivestreams.Subscription;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Command(name = "benchmark",
    showDefaultValues = true, 
    defaultValueProvider = InitialValueAsDefaultValueProvider.class)
public class App  extends Options implements Callable<Void> {

  private static final Logger logger = LogManager.getLogger();

  protected final PrintStream out;
  protected final PrintStream err;
  protected final InputStream in;
  private final String[] args;

  /**
   * Entry point.
   */
  public static void main(String... args) {
    try {
      new App(args).run();
    } catch (Error ex) {
      logger.fatal("Error: ", ex);
      throw ex;
    } catch (Throwable ex) {
      logger.error("Error: ", ex);
      System.exit(1);
    }
  }

  /**
   * Application test entry point.
   */
  public static App test(String... args) throws Exception {
    return new App(args).run();
  }

  private App(String... args) {
    this(new Config(), args);
  }

  private App(Config config, String... args) {
    super(config);
    this.out = System.out;
    this.err = System.err;
    this.in = System.in;
    this.args = args;
  }

  @Override
  public Void call() throws Exception {
    try {
      commonExecute();
    } catch (Error ex) {
      throw ex;
    } catch (Exception ex) {
      throw ex;
    } catch (Throwable ex) {
      throw new Error("An error occurred", ex);
    }
    return null;
  }

  private void commonExecute() throws Exception {
    if (isTest()) {
      return;
    }
    if (isHelpRequested()) {
      getConfiguredCommandLine(this).usage(out);
      return;
    }
    if (isVersionRequested()) {
      out.println(Version.getVersion());
      return;
    }
    execute();
  }

  @SuppressWarnings("resource")
  private void execute() throws Exception {
    try (Closer closer = Closer.create()) {
      updateLogLevel(closer);

      final BenchmarkRunner benchmark;
      
      if (getConfig().getTargetType().equals("mongo")) {
        benchmark = createMongoBenchmark(closer);
      } else if (getConfig().getTargetType().equals("postgres")) {
        benchmark = createPostgresBenchmark(closer);
      } else {
        throw new IllegalArgumentException("Unknown benchmark target type " 
            + getConfig().getTargetType() + ". Must be postgres or mongo");
      }
      
      if (!getConfig().isSkipSetup()) {
        logger.info("Benchmark setup");
        benchmark.setup();
      }
      
      if (getConfig().isOnlySetup()) {
        logger.info("Only setup, skipping benchmark");
        return;
      }
      
      Scheduler scheduler = Schedulers.newParallel(
          "benchmark", getConfig().getParallelism(), false);
      closer.register(() -> Unchecked.runnable(() -> scheduler.dispose()).run());
      CompletableFuture<Void> termination = new CompletableFuture<Void>();

      if (!getConfig().getMetricsAsDuration().orElse(Duration.ZERO).isZero()) {
        logger.info("Starting collecting metrics");
        startMetrics(closer);
      }

      AppSubscriber future = Flux.range(0, 
          getConfig().getIterations() != null 
          ? getConfig().getIterations() : Integer.MAX_VALUE)
          .parallel(getConfig().getParallelism())
          .runOn(scheduler)
          .doOnNext(Unchecked.consumer(ii -> benchmark.run()))
          .sequential()
          .doOnDiscard(Integer.class, i -> termination.complete(null))
          .doOnCancel(() -> termination.complete(null))
          .doOnTerminate(() -> termination.complete(null))
          .subscribeWith(new AppSubscriber());
      try {
        logger.info("Benchmark started");

        if (getConfig().isDisableTransaction()) {
          logger.info("Transactions are disabled");
        }
        
        if (getConfig().getIterations() != null) {
          logger.info("Iterations: " + getConfig().getIterations());
        }
        if (getConfig().getDurationAsDuration().isPresent()) {
          logger.info("Duration: " + getConfig().getDurationAsDuration().get());
          try {
            future.get(getConfig().getDurationAsDuration().get().toSeconds(), TimeUnit.SECONDS);
          } catch (TimeoutException ex) {
            benchmark.close();
            return;
          }
        } else {
          future.get();
        }
      } finally {
        if (!future.isDone()) {
          future.cancel();
        }
        termination.get();
        logger.info("Benchmark completed");
      }
    }
  }

  private void startMetrics(Closer closer) {
    Closeable metricsReporter;
    switch (getConfig().getMetricsReporterAsEnum()) {
      case CSV:
        metricsReporter = MetricsManager.startCsvReporter(getConfig().getMetricsAsDuration()
            .get().getSeconds(), TimeUnit.SECONDS, 
            getConfig().getMetricsFilterAsImmutableList().isEmpty() 
            ? MetricFilter.ALL 
            : (name, metric) -> getConfig()
              .getMetricsFilterAsImmutableList().contains(name));
        break;
      case JXM:
        metricsReporter = MetricsManager.startJmxResporter();
        break;
      case LOG:
        metricsReporter = MetricsManager.startSlf4jReporter(getConfig().getMetricsAsDuration()
            .get().getSeconds(), TimeUnit.SECONDS, 
            getConfig().getMetricsFilterAsImmutableList().isEmpty() 
                ? MetricFilter.ALL 
                : (name, metric) -> getConfig()
                  .getMetricsFilterAsImmutableList().contains(name));
        break;
      default:
        throw new IllegalArgumentException();
    }
    closer.register(metricsReporter);
  }

  public class AppSubscriber extends BaseSubscriber<Object> implements Future<Void> {
    private final CompletableFuture<Void> future = new CompletableFuture<Void>();
    
    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(getConfig().getParallelism());
    }

    @Override
    protected void hookOnNext(Object value) {
      request(1);
    }

    @Override
    protected void hookOnComplete() {
      future.complete(null);
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      future.completeExceptionally(throwable);
      if (throwable instanceof RuntimeException) {
        throw (RuntimeException) throwable;
      }
      throw new RuntimeException(throwable);
    }

    @Override
    protected void hookOnCancel() {
      future.cancel(true);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      return future.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return future.get(timeout, unit);
    }
  }

  private BenchmarkRunner createPostgresBenchmark(Closer closer) {
    Properties jdbcProperties = new Properties();
    PGProperty.PG_HOST.set(jdbcProperties, getConfig().getTarget().getDatabase().getHost());
    PGProperty.PG_PORT.set(jdbcProperties, getConfig().getTarget().getDatabase().getPort());
    PGProperty.PG_DBNAME.set(jdbcProperties, getConfig().getTarget().getDatabase().getName());
    PGProperty.USER.set(jdbcProperties, getConfig().getTarget().getDatabase().getUser());
    PGProperty.PASSWORD.set(jdbcProperties, getConfig().getTarget().getDatabase().getPassword());
    HikariConfig config = new HikariConfig();
    config.setMinimumIdle(getConfig().getMinConnections());
    config.setMaximumPoolSize(getConfig().getMaxConnections());
    config.setConnectionTimeout(getConfig().getConnectionWaitTimeoutAsDuration().toMillis());
    config.setIdleTimeout(getConfig().getConnectionIdleTimeoutAsDuration().toMillis());
    ConnectionSupplier connectionSupplier =
        new HikariConnectionSupplier(new PostgresConnectionSupplier(jdbcProperties) {
          @Override
          public boolean isAutoCommit() {
            return getConfig().isDisableTransaction();
          }

          @Override
          public int getTransactionIsolationLevel() {
            return getConfig().getSqlIsolationLevelAsInt();
          }
        }, config);
    PostgresFlightBenchmark benchmark = PostgresFlightBenchmark.create(connectionSupplier,
        getConfig());
    closer.register(() -> Unchecked.runnable(() -> benchmark.close()).run());
    return new BenchmarkRunner(benchmark);
  }

  private BenchmarkRunner createMongoBenchmark(Closer closer) {
    MongoClient client = MongoClients.create(MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString("mongodb://"
            + (getConfig().getTarget().getDatabase().getUser().isEmpty() ? "" 
                : getConfig().getTarget().getDatabase().getUser() 
                + ":" + getConfig().getTarget().getDatabase().getPassword()
                + "@")
            + getConfig().getTarget().getDatabase().getHost() 
            + ":" + getConfig().getTarget().getDatabase().getPort() + "/benchmark")  )
        .applyToConnectionPoolSettings(builder -> builder
            .minSize(getConfig().getMinConnections())
            .maxSize(getConfig().getMaxConnections())
            .maxWaitTime(getConfig().getConnectionWaitTimeoutAsDuration().toMillis(), 
                TimeUnit.MILLISECONDS)
            .maxConnectionIdleTime(getConfig().getConnectionIdleTimeoutAsDuration().toMillis(), 
                TimeUnit.MILLISECONDS))
        .build());
    MongoFlightBenchmark benchmark = MongoFlightBenchmark.create(client, 
        getConfig());
    closer.register(() -> Unchecked.runnable(() -> benchmark.close()).run());
    return new BenchmarkRunner(benchmark);
  }

  private void updateLogLevel(Closer closer) {
    if (getConfig().getLogLevel() != null) {
      Level previousLevel = LogManager.getLogger("com.ongres.benchmark").getLevel();
      Configurator.setLevel("com.ongres.benchmark", getConfig().getLogLevelAsEnum().getLogLevel());
      closer.register(() -> Configurator.setLevel("com.ongres.benchmark", previousLevel));
    }
  }

  /**
   * Run the application.
   */
  public App run() throws Exception {
    loadConfigFile();
    Iterator<CommandLine> commandLineIterator = newParsedCommandLine().iterator();

    while (commandLineIterator.hasNext()) {
      App app = commandLineIterator.next().getCommand();
      if (!commandLineIterator.hasNext()) {
        if (app.isTest()) {
          return app;
        }
        app.call();
        return app;
      }
    }
    throw new IllegalStateException();
  }

  private void loadConfigFile() {
    CommandLine commandLine = new CommandLine(this);
    commandLine.getCommandSpec().parser().collectErrors(true);
    commandLine.parse(args);
  }

  private List<CommandLine> newParsedCommandLine() throws IOException {
    Config config = new Config();
    if (getConfigFile() != null) {
      config = ConfigUtils.parseConfig(getConfigFile());
    }
    CommandLine configuredCommandLine = getConfiguredCommandLine(config);
    return configuredCommandLine.parse(args);
  }

  protected CommandLine getConfiguredCommandLine(App app) {
    CommandLine configuredCommandLine = new CommandLine(app);
    return configuredCommandLine;
  }

  private CommandLine getConfiguredCommandLine(Config config) {
    App configuredApp = new App(config, args);
    return getConfiguredCommandLine(configuredApp);
  }

  protected static class InitialValueAsDefaultValueProvider implements IDefaultValueProvider {
    @Override
    public String defaultValue(ArgSpec argSpec) throws Exception {
      return Optional.ofNullable(argSpec.initialValue()).map(arg -> arg.toString()).orElse(null);
    }
  }
}
