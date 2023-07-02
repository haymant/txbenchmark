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

package com.ongres.benchmark.jdbc;

import java.sql.Connection;
import java.util.Properties;

import org.postgresql.PGProperty;

public class PostgresConnectionSupplier extends AbstractPoolableConnectionSupplier {

  /**
   * Connection supplier that configure properly the connection for applier.
   */
  public PostgresConnectionSupplier(Properties jdbcProperties) {
    super(jdbcProperties);
  }

  @Override
  protected String getJdbcUrl(Properties jdbcDriverProperties) {
    return "jdbc:postgresql://"
        + PGProperty.PG_HOST.getOrDefault(jdbcDriverProperties) + ":"
        + PGProperty.PG_PORT.getOrDefault(jdbcDriverProperties) + "/"
        + PGProperty.PG_DBNAME.getOrDefault(jdbcDriverProperties);
  }

  @Override
  protected void setupJdbcDriver(Properties jdbcDriverProperties) {
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(jdbcDriverProperties, "9.4");
    PGProperty.STRING_TYPE.set(jdbcDriverProperties, "unspecified");
    PGProperty.REWRITE_BATCHED_INSERTS.set(jdbcDriverProperties, "true");
    PGProperty.APPLICATION_NAME.set(jdbcDriverProperties, "benchmark");
    jdbcDriverProperties.setProperty("stringType", "unspecified");
  }

  @Override
  public boolean isAutoCommit() {
    return false;
  }

  @Override
  public String getInitSql() {
    return "SET TIME ZONE 'UTC'";
  }

  @Override
  public int getTransactionIsolationLevel() {
    return Connection.TRANSACTION_REPEATABLE_READ;
  }

}
