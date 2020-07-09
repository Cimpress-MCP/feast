/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.storage.connectors.jdbc.writer;

import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import feast.storage.connectors.jdbc.postgres.PostgresqlTemplater;
import feast.storage.connectors.jdbc.snowflake.SnowflakeTemplater;
import feast.storage.connectors.jdbc.sqlite.SqliteTemplater;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;

public class JdbcFeatureSink implements FeatureSink {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcFeatureSink.class);

  private final Map<String, FeatureSetProto.FeatureSet> subscribedFeatureSets = new HashMap<>();
  private final StoreProto.Store.JdbcConfig config;

  public JdbcTemplater getJdbcTemplater() {
    return jdbcTemplater;
  }

  private JdbcTemplater jdbcTemplater;

  public JdbcFeatureSink(JdbcConfig config) {
    this.config = config;
    this.jdbcTemplater = getJdbcTemplaterForClass(config.getClassName());
  }

  private JdbcTemplater getJdbcTemplaterForClass(String className) {
    switch (className) {
      case "org.sqlite.JDBC":
        return new SqliteTemplater();
      case "org.postgresql.Driver":
        return new PostgresqlTemplater();
      case "net.snowflake.client.jdbc.SnowflakeDriver":
        return new SnowflakeTemplater();
      default:
        throw new RuntimeException(
            "JDBC class name was not specified, was incorrect, or had no implementation for templating.");
    }
  }

  public static FeatureSink fromConfig(JdbcConfig config) {
    return new JdbcFeatureSink(config);
  }

  public JdbcConfig getConfig() {
    return config;
  }

  /** @param featureSet Feature set to be written */
  @Override
  public void prepareWrite(FeatureSetProto.FeatureSet featureSet) {
    FeatureSetProto.FeatureSetSpec featureSetSpec = featureSet.getSpec();
    String featureSetKey = getFeatureSetRef(featureSetSpec);
    this.subscribedFeatureSets.put(featureSetKey, featureSet);

    Connection conn = connect(this.getConfig());
    if (tableExists(conn, featureSetSpec, this.getConfig())) {
      updateTable(conn, this.getJdbcTemplater(), featureSetSpec);
    } else {
      createTable(conn, this.getJdbcTemplater(), featureSetSpec);
    }
  }

  private void createTable(
      Connection conn, JdbcTemplater jdbcTemplater, FeatureSetProto.FeatureSetSpec featureSetSpec) {
    String featureSetName = getFeatureSetRef(featureSetSpec);
    String createSqlTableCreationQuery = jdbcTemplater.getTableCreationSql(featureSetSpec);
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(createSqlTableCreationQuery);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Could not create table for feature set %s", featureSetName), e);
    }
  }

  private static void updateTable(
      Connection conn, JdbcTemplater jdbcTemplater, FeatureSetProto.FeatureSetSpec featureSetSpec) {
    String featureSetName = getFeatureSetRef(featureSetSpec);
    log.info(String.format("Updating table for %s", featureSetName));
    try {
      // Get a list of existing columns in the table and their types
      Map<String, String> existingColumns = getExistingColumns(conn, featureSetSpec);

      // Create a SQL migration query to add required columns that don't exist
      String tableMigrationSql =
          jdbcTemplater.getTableMigrationSql(featureSetSpec, existingColumns);

      // Don't apply any changes if none are required
      if (tableMigrationSql.isEmpty()) {
        return;
      }
      Statement statement = conn.createStatement();
      statement.executeUpdate(tableMigrationSql);
      log.info(String.format("Successfully updated table schema for %s", featureSetName));
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Could not update table for feature set %s", featureSetName), e);
    }
  }

  private static Map<String, String> getExistingColumns(
      Connection conn, FeatureSetProto.FeatureSetSpec featureSetSpec) {
    Map<String, String> existingColumnsAndTypes = new HashMap<>();
    try {
      Statement st = conn.createStatement();
      String tableName = JdbcTemplater.getTableName(featureSetSpec);
      ResultSet rs = st.executeQuery(String.format("SELECT * FROM %s WHERE 1 = 0", tableName));
      ResultSetMetaData rsmd = rs.getMetaData();
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        existingColumnsAndTypes.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(i));
      }
    } catch (SQLException e) {
      String featureSetName = getFeatureSetRef(featureSetSpec);
      throw new RuntimeException(
          String.format("Could not determine columns for feature set %s", featureSetName), e);
    }
    return existingColumnsAndTypes;
  }

  private static Connection connect(JdbcConfig config) {
    String username = config.getUsername();
    String password = config.getPassword();
    String className = config.getClassName();
    String url = config.getUrl();
    try {

      if (!username.isEmpty()) {
        if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
          String database = config.getDatabase();
          String schema = config.getSchema();
          Properties props = new Properties();
          props.put("user", username);
          props.put("password", password);
          props.put("db", database);
          props.put("schema", schema);
          Class.forName(className);
          return DriverManager.getConnection(url, props);
        } else {
          Class.forName(className);
          return DriverManager.getConnection(url, username, password);
        }
      }
      return DriverManager.getConnection(url);
    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s", url, className),
          e);
    }
  }

  private static boolean tableExists(
      Connection conn,
      FeatureSetProto.FeatureSetSpec featureSetSpec,
      StoreProto.Store.JdbcConfig config) {
    String tableName = JdbcTemplater.getTableName(featureSetSpec);

    String featureSetRef = getFeatureSetRef(featureSetSpec);
    try {
      if (tableName.isEmpty()) {
        throw new RuntimeException(
            String.format("Table name could not be determined for %s", featureSetRef));
      }
      DatabaseMetaData md = conn.getMetaData();
      if (config.getClassName() == "net.snowflake.client.jdbc.SnowflakeDriver") {
        tableName = tableName.toUpperCase();
      }
      ResultSet rs = md.getTables(null, null, tableName, null);
      return rs.next();

    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Could not determine if table %s exists", tableName), e);
    }
  }

  public static String getFeatureSetRef(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }

  public Map<String, FeatureSetProto.FeatureSet> getSubscribedFeatureSets() {
    return subscribedFeatureSets;
  }

  @Override
  public JdbcWrite writer() {
    return new JdbcWrite(
        this.getConfig(), this.getJdbcTemplater(), this.getSubscribedFeatureSets());
  }
}
