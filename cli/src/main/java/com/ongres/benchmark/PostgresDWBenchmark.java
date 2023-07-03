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
import com.ongres.benchmark.config.model.Config;
import com.ongres.benchmark.jdbc.ConnectionSupplier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jooq.lambda.Unchecked;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;

public class PostgresDWBenchmark extends Benchmark {

	private final Logger logger = LogManager.getLogger();


	private final ConnectionSupplier connectionSupplier;
	private final Config config;

	private PostgresDWBenchmark(ConnectionSupplier connectionSupplier, Config config) {
		super();
		this.connectionSupplier = connectionSupplier;
		this.config = config;
	}

	/**
	 * Create an instance of {@class MongoFlightBenchmark}.
	 */
	public static PostgresDWBenchmark create(ConnectionSupplier connectionSupplier, Config config) {
		Preconditions.checkArgument(config.getBookingSleep() >= 0);
		Preconditions.checkArgument(config.getDayRange() > 0);
		return new PostgresDWBenchmark(connectionSupplier, config);
	}

	@Override
	public void setup() {
		Unchecked.runnable(this::databaseSetup).run();
	}

	@Override
	protected void iteration() {
		Unchecked.runnable(this::userOperation).run();
	}

	private void databaseSetup() throws Exception {
		try (Connection connection = connectionSupplier.get(); Statement statement = connection.createStatement()) {
			logger.info("Cleanup schema");
			statement.execute("drop table if exists ord");
			statement.execute("drop table if exists product");
			statement.execute("drop table if exists customer");
			statement.execute("drop table if exists sales");

			logger.info("Creating schema");
			statement.execute("CREATE EXTENSION IF NOT EXISTS tablefunc");
			statement.execute("create table product (" + "product_id integer PRIMARY KEY, " + "product_name text, "
					+ "product_category varchar(3), " + "market_id text, " + "min_price integer, " + "price integer, "
					+ "product_status text, " + "cpty_id integer, " + "date timestamp without time zone, "
					+ "product_description text)");
			statement.execute(
					"create table ord (" + "order_id integer PRIMARY KEY, " + "date timestamp without time zone, "
							+ "customer_id integer, " + "status_id integer, " + "total real, " + "sales_id integer, "
							+ "channel_id varchar(6), " + "product_id integer, " + "price real, " + "quantity real, "
							+ "beta real, " + "vega real, " + "theta real, " + "vanna real, " + "gamma real)");
			statement.execute("create table sales (" + "sales_id integer PRIMARY KEY not null," + "first_name text,"
					+ "last_name text," + "email text," + "phone text," + "hire_date timestamp without time zone,"
					+ "job_id varchar(10)," + "salary integer)");
			statement.execute("create table customer (" + "customer_id integer PRIMARY KEY," + "first_name text,"
					+ "last_name text," + "postal_code varchar(10)," + "city text," + "state text," + "country text)");
			if (!config.isDisableTransaction()) {
				connection.commit();
			}
			logger.info("Importing data");
			PgConnection pgConnection = connection.unwrap(PgConnection.class);
			CopyManager copyManager = pgConnection.getCopyAPI();
			copyManager.copyIn("copy product from stdin" + " with csv header delimiter ',' null '\\N'",
					PostgresDWBenchmark.class.getResourceAsStream("/product.csv"));
			copyManager.copyIn("copy ord from stdin" + " with csv header delimiter ',' null '\\N'",
					PostgresDWBenchmark.class.getResourceAsStream("/ord.csv"));
			copyManager.copyIn("copy sales from stdin" + " with csv header delimiter ',' null '\\N'",
					PostgresDWBenchmark.class.getResourceAsStream("/sales.csv"));
			copyManager.copyIn("copy customer from stdin" + " with csv header delimiter ',' null '\\N'",
					PostgresDWBenchmark.class.getResourceAsStream("/customer.csv"));
			if (!config.isDisableTransaction()) {
				connection.commit();
			}
			statement.execute("alter table ord add" + " foreign key (sales_id) references sales(sales_id)");
			statement.execute("alter table ord add" + " foreign key (customer_id) references customer(customer_id)");
			statement.execute("alter table ord add" + " foreign key (product_id) references product(product_id)");
			if (!config.isDisableTransaction()) {
				connection.commit();
			}
		}
	}

	private void userOperation() throws Exception {
		try (Connection connection = connectionSupplier.get()) {
			try {
				getOrders(connection);
				groupset(connection);
				cube(connection);
				rollup(connection);
				pivot(connection);
				
				if (!config.isDisableTransaction()) {
					connection.commit();
				}
			} catch (Exception ex) {
				if (!config.isDisableTransaction()) {
					try {
						connection.rollback();
					} catch (Exception abortEx) {
						logger.error(abortEx);
					}
				}
				if (ex instanceof PSQLException && (((PSQLException) ex).getSQLState().equals("40001"))) {
					throw new RetryUserOperationException(ex);
				}
				if (ex instanceof PSQLException) {
					throw new RuntimeException(
							"PSQLException: " + ex.getMessage() + " (" + ((PSQLException) ex).getSQLState() + ")", ex);
				}
				throw ex;
			}
		}
	}

	private Document getOrders(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						"select *" + " from ord " + " inner join customer on (customer.customer_id = ord.customer_id)"
								+ " inner join sales on (sales.sales_id = ord.sales_id)"
								+ " inner join product on (product.product_id = ord.product_id)" + " limit 100")) {
			Preconditions.checkState(resultSet.next());
			return new Document().append("order_id", resultSet.getString("order_id"))
					.append("customer_id", resultSet.getString("customer_id"))
					.append("product_id", resultSet.getString("product_id"))
					.append("sales_id", resultSet.getString("sales_id"));
		}
	}

	private Document groupset(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						"select customer.first_name as customer, product.product_name as product, sales.first_name as sales, "
								+ " sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, "
								+ " sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna"
								+ " from ord " + " inner join customer on (customer.customer_id = ord.customer_id)"
								+ " inner join sales on (sales.sales_id = ord.sales_id)"
								+ " inner join product on (product.product_id = ord.product_id)"
								+ " group by grouping sets (customer.first_name, product.product_name, sales.first_name)")) {
			Preconditions.checkState(resultSet.next());
			return new Document().append("customer", resultSet.getString("customer"))
					.append("product", resultSet.getString("product")).append("sales", resultSet.getString("sales"))
					.append("px", resultSet.getString("px"));
		}
	}
	
	private Document cube(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						"select customer.first_name as customer, product.product_name as product, sales.first_name as sales, "
								+ " sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, "
								+ " sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna"
								+ " from ord " + " inner join customer on (customer.customer_id = ord.customer_id)"
								+ " inner join sales on (sales.sales_id = ord.sales_id)"
								+ " inner join product on (product.product_id = ord.product_id)"
								+ " group by cube (customer.first_name, product.product_name, sales.first_name)")) {
			Preconditions.checkState(resultSet.next());
			return new Document().append("customer", resultSet.getString("customer"))
					.append("product", resultSet.getString("product")).append("sales", resultSet.getString("sales"))
					.append("px", resultSet.getString("px"));
		}
	}
	
	private Document rollup(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						"select customer.first_name as customer, product.product_name as product, sales.first_name as sales, "
								+ " sum(ord.price) as px, sum(ord.quantity) as amount, sum(ord.beta) as beta, "
								+ " sum(ord.gamma) as gamma, sum(ord.theta) as theta, sum(ord.vega) as vega, sum(ord.vanna) as vanna"
								+ " from ord " + " inner join customer on (customer.customer_id = ord.customer_id)"
								+ " inner join sales on (sales.sales_id = ord.sales_id)"
								+ " inner join product on (product.product_id = ord.product_id)"
								+ " group by rollup (customer.first_name, product.product_name, sales.first_name)")) {
			Preconditions.checkState(resultSet.next());
			return new Document().append("customer", resultSet.getString("customer"))
					.append("product", resultSet.getString("product")).append("sales", resultSet.getString("sales"))
					.append("px", resultSet.getString("px"));
		}
	}
	
	// pivot to get products sold by each sales
	private Document pivot(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(
						" select * from crosstab('"
						+ "select product.product_name as product, sales.first_name as sales, "
						+ "ord.quantity as amount from ord inner join sales on (sales.sales_id = ord.sales_id) "
						+ "inner join product on (product.product_id = ord.product_id) order by 1, 2') as "
						+ "sales (product text, sales1 real, sales2 real, sales3 real)")) {
			Preconditions.checkState(resultSet.next());
			return new Document().append("product", resultSet.getString("product"))
					.append("sales1", resultSet.getString("sales1")).append("sales2", resultSet.getString("sales2"))
					.append("sales3", resultSet.getString("sales3"));
		}
	}

	@Override
	protected void internalClose() throws Exception {
		connectionSupplier.close();
	}
}
