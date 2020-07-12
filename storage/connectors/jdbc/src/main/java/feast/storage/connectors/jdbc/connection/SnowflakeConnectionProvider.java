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

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import feast.proto.core.StoreProto;

public class SnowflakeConnectionProvider implements JdbcConnectionProvider {
  private final String database;
  private final String schema;
  private final String password;
  private final String username;
  private final String url;
  private final String className;
  private String warehouse = "";

  public SnowflakeConnectionProvider(Map<String, String> config) {
    if (!config.containsKey("database")
        || !config.containsKey("schema")
        || !config.containsKey("class_name")
        || !config.containsKey("url")
        || !config.containsKey("username")
        || !config.containsKey("password")) {
      throw new IllegalArgumentException(
          String.format(
              "SnowflakeConnectionProvider config missing one or more fields! "
                  + "Database: %s Schema: %s ClassName: %s Url: %s Username: %s Password: MASKED ",
              config.get("database"),
              config.get("schema"),
              config.get("class_name"),
              config.get("url"),
              config.get("username")));
    }
    this.database = config.get("database");
    this.schema = config.get("schema");
    this.className = config.get("class_name");
    this.url = config.get("url");
    this.username = config.get("username");
    this.password = config.get("password");
    this.warehouse = config.get("warehouse");
  }

  @Override
  public Connection getConnection() {
    try {
      Class.forName(this.className);
      // db and schema are required for snowflake connection
      Properties props = new Properties();
      props.put("user", this.username);
      props.put("password", this.password);
      props.put("db", this.database);
      props.put("schema", this.schema);
      return DriverManager.getConnection(this.url, props);

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s wuth database %s, and schema %s",
              this.url, this.className, this.database, this.schema),
          e);
    }
  }
  
  @Override
  public JdbcIO.DataSourceConfiguration getdsconfig(){
	      return JdbcIO.DataSourceConfiguration.create(this.className, this.url)
	          .withUsername(!this.username.isEmpty() ? this.username : null)
	          .withPassword(!this.password.isEmpty() ? this.password : null)
	          .withConnectionProperties(
	              String.format("warehouse=%s;db=%s;schema=%s", this.warehouse, this.database, this.schema));
	    
  }
}