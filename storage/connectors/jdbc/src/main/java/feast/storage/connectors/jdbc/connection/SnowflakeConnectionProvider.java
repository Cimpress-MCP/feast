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

public class SnowflakeConnectionProvider implements JdbcConnectionProvider {
  private final Map<String, String> config;

  public SnowflakeConnectionProvider(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public Connection getConnection() {
    try {
      Class.forName(this.config.get("class_name"));
      // db and schema are required for snowflake connection
      Properties props = new Properties();
      props.put("user", this.config.get("username"));
      props.put("password", this.config.get("password"));
      props.put("db", this.config.get("database"));
      props.put("schema", this.config.get("schema"));
      props.put("role", this.config.get("role"));
      return DriverManager.getConnection(this.config.get("url"), props);

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format("Could not connect to database with the given config: %s", this.config), e);
    }
  }
}
