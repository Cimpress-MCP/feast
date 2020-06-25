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
package feast.storage.connectors.jdbc.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class PostgresConnectionProvider implements JdbcConnectionProvider {
  private final String database;
  private final String schema;
  private final String password;
  private final String username;
  private final String url;
  private final String className;

  public PostgresConnectionProvider(Map<String, String> config) {
    if (!config.containsKey("url") || !config.containsKey("username")) {
      throw new IllegalArgumentException(
          "SnowflakeConnectionProvider config missing one or more fields!");
    }
    this.database = config.getOrDefault("database", null);
    this.schema = config.getOrDefault("schema", null);
    this.className = config.getOrDefault("class_name", null);
    this.url = config.get("url");
    this.username = config.get("username");
    this.password = config.getOrDefault("password", null);
  }

  @Override
  public Connection getConnection() {
    try {
      Class.forName(this.className);
      if (!this.database.isEmpty() && !this.schema.isEmpty()) {
        // snowflake database must config database and schema
        Properties props = new Properties();
        props.put("user", this.username);
        props.put("password", this.password);
        props.put("db", this.database);
        props.put("schema", this.schema);
        return DriverManager.getConnection(this.url, props);
      } else {
        return DriverManager.getConnection(this.url, this.username, this.password);
      }

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s wuth database %s, and schema %s",
              this.url, this.className, this.database, this.schema),
          e);
    }
  }
}
