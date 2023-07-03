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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Variable;
import com.ongres.benchmark.config.model.Config;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Builder;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jooq.lambda.Unchecked;

public class MongoDWBenchmark extends Benchmark {


  private final Logger logger = LogManager.getLogger();

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
    Builder csvFormatBuilder = CSVFormat.DEFAULT.builder()
    		.setDelimiter(",").setNullString("\\N")
    		.setSkipHeaderRecord(true);
    logger.info("Importing orders");
    database.createCollection("ord");
    MongoCollection<Document> ord = database.getCollection("ord");
    String numCol = "price,quantity,beta,vega,theta,vanna,gamma";
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/ord.csv"), 
        StandardCharsets.UTF_8, csvFormatBuilder
        .setHeader("order_id", "date", "customer_id", "status_id", "total", "sales_id", "channel_id",
        		"product_id", "price", "quantity", "beta", "vega", "theta", "vanna", "gamma").build())
        .forEach(record -> {
        	ord.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), 
            		e -> numCol.indexOf(e.getKey())>=0 ? Double.parseDouble(e.getValue()): e.getValue() ))));
        });
    ord.createIndex(Indexes.ascending("order_id"));
    logger.info("Importing product");
    database.createCollection("product");
    MongoCollection<Document> product = database.getCollection("product");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/product.csv"), 
        StandardCharsets.UTF_8, csvFormatBuilder
        .setHeader("product_id", "product_name", "product_category", "market_id", "min_price", "price", "product_status",
        		"cpty_id", "date", "product_description").build())
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
        StandardCharsets.UTF_8, csvFormatBuilder
        .setHeader("sales_id", "first_name", "last_name", "email", "phone", "hire_date", "job_id", "salary").build())
        .forEach(record -> sales.insertOne(new Document(
            record.toMap().entrySet().stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))));
    sales.createIndex(Indexes.ascending("sales_id"));
    logger.info("Importing customer");
    database.createCollection("customer");
    MongoCollection<Document> customer = database.getCollection("customer");
    CSVParser.parse(
        MongoDWBenchmark.class.getResourceAsStream("/customer.csv"), 
        StandardCharsets.UTF_8, csvFormatBuilder
        .setHeader("customer_id", "first_name", "last_name", "postal_code", "city", "state", "country").build())
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
        getOrders(session);
        //group(session);
    	rollup(session);
    	cube(session);
        //pivot(session);
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
    AggregateIterable<Document> orders = database.getCollection("ord")
        .aggregate(session,
            getOrderAggregate());
    return orders.first();
  }


  // https://www.mongodb.com/docs/drivers/java/sync/current/fundamentals/builders/aggregates/#group
  private List<Bson> getOrderAggregate() {
	  List<Variable<String>> variables = Arrays.asList(
			  new Variable<>("pId", "$product_id"),
			  new Variable<>("cId", "$customer_id"),
			  new Variable<>("sId", "$sales_id"));
	  List<Bson> pPipeline = Arrays.asList(
			  Aggregates.match(Filters.expr(new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$product_id", "$$pId")) )))),
			  Aggregates.project(Projections.fields(Projections.include("product_name"), Projections.excludeId()))			  );
	  List<Bson> cPipeline = Arrays.asList(
			  Aggregates.match(Filters.expr(new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$customer_id", "$$cId")) )))),
			  Aggregates.project(Projections.fields(Projections.include("first_name"), Projections.excludeId()))			  );
	  List<Bson> sPipeline = Arrays.asList(
			  Aggregates.match(Filters.expr(new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$sales_id", "$$sId")) )))),
			  Aggregates.project(Projections.fields(Projections.include("first_name"), Projections.excludeId()))			  );	  
	  List<Bson> innerJoinLookup = Arrays.asList(
			  Aggregates.lookup("product", variables, pPipeline, "prod"),
			  Aggregates.lookup("customer", variables, cPipeline, "cust"),
			  Aggregates.lookup("sales", variables, sPipeline, "sale")
			  );
	  return innerJoinLookup;
  }

	private Document group(ClientSession session) {
		List<Variable<String>> variables = Arrays.asList(new Variable<>("pId", "$product_id"),
				new Variable<>("cId", "$customer_id"), new Variable<>("sId", "$sales_id"));
		List<Bson> pPipeline = Arrays.asList(
				Aggregates.match(Filters.expr(new Document("$and",
						Arrays.asList(new Document("$eq", Arrays.asList("$product_id", "$$pId")))))),
				Aggregates.project(Projections.fields(Projections.include("product_name"), Projections.excludeId())));
		List<Bson> cPipeline = Arrays.asList(
				Aggregates.match(Filters.expr(new Document("$and",
						Arrays.asList(new Document("$eq", Arrays.asList("$customer_id", "$$cId")))))),
				Aggregates.project(Projections.fields(Projections.include("first_name"), Projections.excludeId())));
		List<Bson> sPipeline = Arrays.asList(
				Aggregates.match(Filters.expr(
						new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$sales_id", "$$sId")))))),
				Aggregates.project(Projections.fields(Projections.include("first_name"), Projections.excludeId())));
		
		Document groups = new Document()
				.append("$group", new Document("_id", new Document()
						.append("prod", "$prod.product_name")
						.append("custo", "$cust.first_name")
						.append("sal", "$sale.first_name"))
						.append("beta", new Document("$sum", "$beta"))
						.append("gamma", new Document("$sum", "$gamma"))
						.append("theta", new Document("$sum", "$theta"))
						.append("vega", new Document("$sum", "$vega"))
						.append("vanna", new Document("$sum", "$vanna"))
						.append("quantity", new Document("$sum", "$quantity")));
		List<Bson> innerJoinLookup = Arrays.asList(Aggregates.lookup("product", variables, pPipeline, "prod"),
				Aggregates.lookup("customer", variables, cPipeline, "cust"),
				Aggregates.lookup("sales", variables, sPipeline, "sale"),
				groups
				);

		AggregateIterable<Document> orders = database.getCollection("ord").aggregate(session, innerJoinLookup);
		return orders.first();
	}
  
	private Document cube(ClientSession session) {
		return null;
	}

	private Document rollup(ClientSession session) {
		return null;
	}
  
	private Document pivot(ClientSession session) {
		List<Variable<String>> variables = Arrays.asList(new Variable<>("pId", "$product_id"),
				new Variable<>("sId", "$sales_id"));
		List<Bson> pPipeline = Arrays.asList(
				Aggregates.match(Filters.expr(new Document("$and",
						Arrays.asList(new Document("$eq", Arrays.asList("$product_id", "$$pId")))))),
				Aggregates.project(Projections.fields(Projections.include("product_name"), Projections.excludeId())));
		List<Bson> sPipeline = Arrays.asList(
				Aggregates.match(Filters.expr(
						new Document("$and", Arrays.asList(new Document("$eq", Arrays.asList("$sales_id", "$$sId")))))),
				Aggregates.project(Projections.fields(Projections.include("first_name"), Projections.excludeId())));
		Document groups = new Document().append("$group",
				new Document("_id", new Document().append("prod", "$prod.product_name")
						.append("sal", "$sale.first_name"))
						.append("items", new Document("$addToSet", 
								new Document().append("name", "$prod.product_name")
								.append("value", "$sale.first_name"))));
		Document project = new Document("$project", new Document("tmp", new Document("$arrayToObject", new Document("$zip", ""))));
		List<Bson> innerJoinLookup = Arrays.asList(Aggregates.lookup("product", variables, pPipeline, "prod"),
				Aggregates.lookup("sales", variables, sPipeline, "sale"), groups);
		AggregateIterable<Document> orders = database.getCollection("ord").aggregate(session, innerJoinLookup);
		return orders.first();
	}

  @Override
  protected void internalClose() throws Exception {
    client.close();
  }
}
