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
package feast.storage.connectors.jdbc.snowflake;

import feast.proto.core.StoreProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.util.*;

public class SnowflakeTemplater implements JdbcTemplater {

  /** */
  private static final long serialVersionUID = 1L;

  @Override
  public String getTableCreationSql(StoreProto.Store.JdbcConfig config) {
    StringJoiner columnsAndTypesSQL = new StringJoiner(", ");
    Map<String, String> requiredColumns = getRequiredColumns();
    for (String column : requiredColumns.keySet()) {

      String type = requiredColumns.get(column);
      columnsAndTypesSQL.add(String.format("%s %s", column, type));
    }
    String createTableStatement =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s.%s (%s);",
            config.getDatabase(), config.getSchema(), config.getTableName(), columnsAndTypesSQL);

    return createTableStatement;
  }

  @Override
  public Map<String, String> getRequiredColumns() {
    Map<String, String> requiredColumns = new LinkedHashMap<>();

    requiredColumns.put("event_timestamp", "TIMESTAMP_LTZ");
    requiredColumns.put("created_timestamp", "TIMESTAMP_LTZ");
    requiredColumns.put("project", "VARCHAR");
    requiredColumns.put("featureset", "VARCHAR");
    requiredColumns.put("feature", "VARIANT");
    requiredColumns.put("ingestion_id", "VARCHAR");
    requiredColumns.put("job_id", "VARCHAR");
    return requiredColumns;
  }

  @Override
  public String getFeatureRowInsertSql(String tableName) {

    StringJoiner columnsSql = new StringJoiner(",");
    StringJoiner valueSql = new StringJoiner(",");

    Map<String, String> requiredColumns = getRequiredColumns();
    for (String column : requiredColumns.keySet()) {
      columnsSql.add(column);
      if (column == "feature") {
        valueSql.add("parse_json(?)");
      } else {
        valueSql.add("?");
      }
    }

    return String.format("INSERT INTO %s (%s) select %s;", tableName, columnsSql, valueSql);
  }
}
