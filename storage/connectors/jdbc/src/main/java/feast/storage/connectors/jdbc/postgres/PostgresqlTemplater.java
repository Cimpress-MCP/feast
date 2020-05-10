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
package feast.storage.connectors.jdbc.postgres;

import feast.proto.core.FeatureSetProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.util.*;
import org.slf4j.Logger;

public class PostgresqlTemplater implements JdbcTemplater {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(PostgresqlTemplater.class);

  @Override
  public String getTableCreationSql(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    StringJoiner columnsAndTypesSQL = new StringJoiner(", ");
    Map<String, String> requiredColumns = getRequiredColumns(featureSetSpec);

    for (String column : requiredColumns.keySet()) {
      String type = requiredColumns.get(column);
      columnsAndTypesSQL.add(String.format("%s %s", column, type));
    }

    String createTableStatement =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s);",
            JdbcTemplater.getTableName(featureSetSpec), columnsAndTypesSQL);
    log.debug(createTableStatement);
    return createTableStatement;
  }

  @Override
  public Map<String, String> getRequiredColumns(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    Map<String, String> requiredColumns = new LinkedHashMap<>();

    requiredColumns.put("event_timestamp", "TIMESTAMP");
    requiredColumns.put("created_timestamp", "TIMESTAMP");

    for (FeatureSetProto.EntitySpec entity : featureSetSpec.getEntitiesList()) {
      requiredColumns.put(entity.getName(), PostgresqlTypeUtil.toSqlType(entity.getValueType()));
    }

    requiredColumns.put("ingestion_id", "VARCHAR");
    requiredColumns.put("job_id", "VARCHAR");

    for (FeatureSetProto.FeatureSpec feature : featureSetSpec.getFeaturesList()) {
      requiredColumns.put(feature.getName(), PostgresqlTypeUtil.toSqlType(feature.getValueType()));
    }

    return requiredColumns;
  }

  @Override
  public String getTableMigrationSql(
      FeatureSetProto.FeatureSetSpec featureSetSpec, Map<String, String> existingColumns) {
    Map<String, String> requiredColumns = getRequiredColumns(featureSetSpec);
    String tableName = JdbcTemplater.getTableName(featureSetSpec);

    // Filter required columns down to only the ones that don't exist
    for (String existingColumn : existingColumns.keySet()) {
      if (!requiredColumns.containsKey(existingColumn)) {
        throw new RuntimeException(
            String.format(
                "Found column %s in table %s that should not exist", existingColumn, tableName));
      }
      requiredColumns.remove(existingColumn);
    }

    if (requiredColumns.size() == 0) {
      log.info(
          String.format("All columns already exist for table %s, no update necessary.", tableName));
      return "";
    }

    StringJoiner addColumnSql = new StringJoiner(", ");
    // Filter required columns down to only the ones we need to add
    for (String requiredColumn : requiredColumns.keySet()) {
      String requiredColumnType = requiredColumns.get(requiredColumn);
      addColumnSql.add(String.format("ADD COLUMN %s %s", requiredColumn, requiredColumnType));
    }

    String tableMigrationSql = String.format("ALTER TABLE %s %s", tableName, addColumnSql);
    log.debug(tableMigrationSql);
    return tableMigrationSql;
  }

  public String getFeatureRowInsertSql(FeatureSetProto.FeatureSetSpec featureSetSpec) {

    StringJoiner columnsSql = new StringJoiner(",");
    StringJoiner valueSql = new StringJoiner(",");

    Map<String, String> requiredColumns = getRequiredColumns(featureSetSpec);

    for (String column : requiredColumns.keySet()) {
      columnsSql.add(column);
      valueSql.add("?");
    }

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)",
        JdbcTemplater.getTableName(featureSetSpec), columnsSql, valueSql);
  }
}
