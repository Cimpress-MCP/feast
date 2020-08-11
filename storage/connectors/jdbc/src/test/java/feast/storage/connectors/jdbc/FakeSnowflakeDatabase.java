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
package feast.storage.connectors.jdbc;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;

public class FakeSnowflakeDatabase implements Serializable {
  private static Map<String, List<List<String>>> tables = new HashMap<>();

  private FakeSnowflakeDatabase() {
    tables = new HashMap<>();
  }

  /**
   * Create a empty table
   *
   * @param table: table name, where each table is a list of string
   */
  public static void createTable(String table) {
    FakeSnowflakeDatabase.tables.put(table, Collections.emptyList());
  }

  public static void createTable(String table, List<List<String>> rows) {
    FakeSnowflakeDatabase.tables.put(table, rows);
  }

  public static List<List<String>> getTable(String table) throws SnowflakeSQLException {
    if (!FakeSnowflakeDatabase.tables.containsKey(table)) {
      throw new SnowflakeSQLException(
          "Fake query id", "SQL compilation error: Table does not exist", table, 0);
    }
    return FakeSnowflakeDatabase.tables.get(table);
  }

  public static List<List<String>> executeQuery(String query) throws SnowflakeSQLException {
    if (query.startsWith("SELECT * FROM")) {
      String table = query.replace("SELECT * FROM ", "");
      return getTable(table);
    }
    // TODO: add more cases
    return null;
  }
}
