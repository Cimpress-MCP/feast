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
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
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

  @Override
  public void setSinkParameters(
      FeatureRow element,
      PreparedStatement preparedStatement,
      String jobName,
      FeatureSetProto.FeatureSetSpec currentFeatureSetSpec) {
    try {

      Map<String, ValueProto.Value> fieldMap =
          element.getFieldsList().stream()
              .collect(Collectors.toMap(FieldProto.Field::getName, FieldProto.Field::getValue));

      // Set event_timestamp
      Instant eventTsInstant =
          Instant.ofEpochSecond(element.getEventTimestamp().getSeconds())
              .plusNanos(element.getEventTimestamp().getNanos());

      preparedStatement.setTimestamp(
          1, Timestamp.from(eventTsInstant), Calendar.getInstance(TimeZone.getTimeZone("UTC")));

      // Set created_timestamp
      preparedStatement.setTimestamp(
          2,
          new Timestamp(System.currentTimeMillis()),
          Calendar.getInstance(TimeZone.getTimeZone("UTC")));

      // entities
      int counter = 3;
      for (FeatureSetProto.EntitySpec entitySpec : currentFeatureSetSpec.getEntitiesList()) {
        ValueProto.Value value = fieldMap.get(entitySpec.getName());
        setPreparedStatementValue(preparedStatement, value, counter);
        counter++;
      }

      // Set ingestion Id
      preparedStatement.setString(counter, element.getIngestionId());
      counter++;

      // Set job Name
      preparedStatement.setString(counter, jobName);
      counter++;

      // feature
      for (FeatureSetProto.FeatureSpec featureSpec : currentFeatureSetSpec.getFeaturesList()) {
        ValueProto.Value value =
            fieldMap.getOrDefault(featureSpec.getName(), ValueProto.Value.getDefaultInstance());
        setPreparedStatementValue(preparedStatement, value, counter);
        counter++;
      }
      preparedStatement.getConnection().commit();
    } catch (SQLException e) {
      log.error(
          String.format(
              "Could not construct prepared statement for JDBC IO. FeatureRow: %s:", element),
          e.getMessage());
    }
  }

  public static void setPreparedStatementValue(
      PreparedStatement preparedStatement, ValueProto.Value value, int position) {
    ValueProto.Value.ValCase protoValueType = value.getValCase();
    try {
      switch (protoValueType) {
        case BYTES_VAL:
          preparedStatement.setBytes(position, value.getBytesVal().toByteArray());
          break;
        case STRING_VAL:
          preparedStatement.setString(position, value.getStringVal());
          break;
        case INT32_VAL:
          preparedStatement.setInt(position, value.getInt32Val());
          break;
        case INT64_VAL:
          preparedStatement.setLong(position, value.getInt64Val());
          break;
        case FLOAT_VAL:
          preparedStatement.setFloat(position, value.getFloatVal());
          break;
        case DOUBLE_VAL:
          preparedStatement.setDouble(position, value.getDoubleVal());
          break;
        case BOOL_VAL:
          preparedStatement.setBoolean(position, value.getBoolVal());
          break;
        case STRING_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getStringListVal().toByteArray()));
          break;
        case BYTES_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getBytesListVal().toByteArray()));
          break;
        case INT64_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getInt64ListVal().toByteArray()));
          break;
        case INT32_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getInt32ListVal().toByteArray()));
          break;
        case FLOAT_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getFloatListVal().toByteArray()));
          break;
        case DOUBLE_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getDoubleListVal().toByteArray()));
          break;
        case BOOL_LIST_VAL:
          preparedStatement.setString(
              position, Base64.getEncoder().encodeToString(value.getBoolListVal().toByteArray()));
          break;
        case VAL_NOT_SET:
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Could not determine field protoValueType for incoming feature row: %s",
                  protoValueType));
      }
    } catch (IllegalArgumentException | SQLException e) {
      log.error(
          String.format(
              "Could not cast value %s of type %s int SQL field: ",
              value.toString(), protoValueType),
          e.getMessage());
    }
  }
}
