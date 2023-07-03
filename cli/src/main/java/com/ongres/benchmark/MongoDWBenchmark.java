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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.ongres.benchmark.config.model.Config;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jooq.lambda.Unchecked;

public class MongoDWBenchmark extends Benchmark {

  private static final int MAX_SCHEDULE_ID = 14185;

  private final Logger logger = LogManager.getLogger();

  private final AtomicLong idGenerator = new AtomicLong(0);
  private final Random random = new Random();
  private final MongoClient client;
  private final MongoDatabase database;
  private final Config config;

  private MongoDWBenchmark(MongoClient client, MongoDatabase database, Config config) {
    super();
    this.client = client;
    this.database = database;
    this.config = config;
  }

  /**
   * Create an instance of {@class MongoFlightBenchmark}.
   */
  public static MongoDWBenchmark create(MongoClient client,
      Config config) {
    Preconditions.checkArgument(config.getBookingSleep() >= 0);
    Preconditions.checkArgument(config.getDayRange() > 0);
    MongoDatabase database = client.getDatabase(config.getTarget().getDatabase().getName());
    return new MongoDWBenchmark(client,
        database,
        config);
  }

  @Override
  public void setup() {
    Unchecked.runnable(this::setupDatabase).run();
  }

  @Override
  protected void iteration() {
	  Unchecked.runnable(this::userOperation).run();
  }

  private void setupDatabase() throws Exception {
    logger.info("Cleanup");
    database.getCollection("ord").drop();
    database.getCollection("product").drop();
    database.getCollection("customer").drop();
    database.getCollection("sales").drop();
    CSVFormat csvFormat = CSVFormat.newFormat(';')
        .withNullString("\\N");
    logger.info("Importing orders");
    database.createCollection("ord");
    MongoCollection<Document> ord = database.getCollection("ord");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/ord.csv"), 
        StandardCharsets.UTF_8, csvFormat
        .withHeader("order_id", "date", "customer_id", "status_id", "total", "sales_id", "channel_id",
        		"product_id", "price", "quantity", "beta", "vega", "theta", "vanna", "gamma"))
        .forEach(record -> ord.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))));
    ord.createIndex(Indexes.ascending("order_id"));
    logger.info("Importing product");
    database.createCollection("product");
    MongoCollection<Document> product = database.getCollection("product");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/product.csv"), 
        StandardCharsets.UTF_8, csvFormat
        .withHeader("product_id", "product_name", "product_category", "market_id", "min_price", "price", "product_status",
        		"cpty_id", "date", "product_description"))
        .forEach(record -> product.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))));
    product.createIndex(Indexes.ascending("product_id"));
    logger.info("Importing sales");
    database.createCollection("sales");
    MongoCollection<Document> sales = database.getCollection("sales");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/sales.csv"), 
        StandardCharsets.UTF_8, csvFormat
        .withHeader("sales_id", "first_name", "last_name", "email", "phone", "hire_date", "job_id", "salary"))
        .forEach(record -> sales.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))));
    sales.createIndex(Indexes.ascending("sales_id"));
    logger.info("Importing customer");
    database.createCollection("customer");
    MongoCollection<Document> customer = database.getCollection("customer");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/product.csv"), 
        StandardCharsets.UTF_8, csvFormat
        .withHeader("customer_id", "first_name", "last_name", "postal_code", "city", "state", "country"))
        .forEach(record -> customer.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))));
    customer.createIndex(Indexes.ascending("customer_id"));    
  }

  private void userOperation() throws Exception {
    try (ClientSession session = client.startSession(
        ClientSessionOptions.builder()
        .causallyConsistent(!config.isMongoNotCasuallyConsistent())
        .build())) {
      session.startTransaction(TransactionOptions.builder()
          .readPreference(config.getMongoReadPreferenceAsReadPreference())
          .readConcern(config.getMongoReadConcernAsReadConcern())
          .writeConcern(config.getMongoWriteConcernAsWriteConcern())
          .build());
      try {
        final Document orders = getOrders(session);
        session.commitTransaction();
      } catch (Exception ex) {
        try {
          session.abortTransaction();
        } catch (Exception abortEx) {
          logger.error(abortEx);
        }
        if (ex instanceof MongoCommandException
            && (((MongoCommandException) ex).hasErrorLabel(
                MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
                || ((MongoCommandException) ex).hasErrorLabel(
                    MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL))) {
          throw new RetryUserOperationException(ex);
        }
        throw ex;
      }
    }
  }



  private Document getOrders(ClientSession session) {
    AggregateIterable<Document> schedules = database.getCollection("schedule")
        .aggregate(session,
            getUserScheduleAggregate());
    return schedules.first();
  }

  private Document getOrders() {
    AggregateIterable<Document> schedules = database.getCollection("schedule")
        .aggregate(
            getUserScheduleAggregate());
    return schedules.first();
  }

  private List<Bson> getUserScheduleAggregate() {
    return Arrays.asList(
        Aggregates.match(Filters.eq("schedule_id", 1)),
        Aggregates.lookup("aircraft", "aircraft", "iata", "aircraft"),
        Aggregates.project(new Document()
            .append("schedule_id", 1)
            .append("duration", 1)
            .append("capacity", "$aircraft.capacity")));
  }


  @Override
  protected void internalClose() throws Exception {
    client.close();
  }
}
