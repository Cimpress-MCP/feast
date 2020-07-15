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

import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.slf4j.Logger;

public class SnowflakeTemplater implements JdbcTemplater {

  /** */
  private static final long serialVersionUID = 1L;

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(SnowflakeTemplater.class);

  @Override
  public String getTableCreationSql(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    StringJoiner columnsAndTypesSQL = new StringJoiner(", ");
//    Map<String, String> requiredColumns = getRequiredColumns(featureSetSpec);
    Map<String, String> requiredColumns = getRequiredColumns();
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

//  @Override
//  public Map<String, String> getRequiredColumns(FeatureSetProto.FeatureSetSpec featureSetSpec) {
//    Map<String, String> requiredColumns = new LinkedHashMap<>();
//
//    requiredColumns.put("event_timestamp", "TIMESTAMP_LTZ");
//    requiredColumns.put("created_timestamp", "TIMESTAMP_LTZ");
//    requiredColumns.put("feature", "VARIANT");
//    requiredColumns.put("ingestion_id", "VARCHAR");
//    requiredColumns.put("job_id", "VARCHAR");
//    return requiredColumns;
//  }
//  
  @Override
  public Map<String, String> getRequiredColumns() {
    Map<String, String> requiredColumns = new LinkedHashMap<>();

    requiredColumns.put("event_timestamp", "TIMESTAMP_LTZ");
    requiredColumns.put("created_timestamp", "TIMESTAMP_LTZ");
    requiredColumns.put("feature", "VARIANT");
    requiredColumns.put("ingestion_id", "VARCHAR");
    requiredColumns.put("job_id", "VARCHAR");
    return requiredColumns;
  }

  public Map<String, String> getSubscribedColumns(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    Map<String, String> subscribedColumns = new LinkedHashMap<>();
    for (FeatureSetProto.EntitySpec entity : featureSetSpec.getEntitiesList()) {
      subscribedColumns.put(
          entity.getName(), SnowflakesqlTypeUtil.toSqlType(entity.getValueType()));
    }

    for (FeatureSetProto.FeatureSpec feature : featureSetSpec.getFeaturesList()) {
      subscribedColumns.put(
          feature.getName(), SnowflakesqlTypeUtil.toSqlType(feature.getValueType()));
    }

    return subscribedColumns;
  }

  @Override
  public String getTableMigrationSql(
      FeatureSetProto.FeatureSetSpec featureSetSpec, Map<String, String> existingColumns) {

    String tableMigrationSql = "";
    return tableMigrationSql;
  }

  public String getFeatureRowInsertSql(String featureSetSpec) {
    StringJoiner columnsSql = new StringJoiner(",");
    StringJoiner valueSql = new StringJoiner(",");
//    Map<String, String> requiredColumns = getRequiredColumns(featureSetSpec);
    Map<String, String> requiredColumns = getRequiredColumns();
    for (String column : requiredColumns.keySet()) {

      columnsSql.add(column);
      if (column == "feature") {
        valueSql.add(" parse_json(?)");
      } else {
        valueSql.add("?");
      }
    }
//    return String.format(
//        "INSERT INTO %s (%s) select %s",
//        JdbcTemplater.getTableName(featureSetSpec), columnsSql, valueSql);
//    
    return String.format(
            "INSERT INTO %s (%s) select %s",
            featureSetSpec, columnsSql, valueSql);
  }
  
  

  public void setSinkParameters(
      FeatureRowProto.FeatureRow element,
      PreparedStatement preparedStatement,
      String jobName) {
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

      // Set Feature
      JSONObject json_variant = new JSONObject();
      for (String row : fieldMap.keySet()) {
        setFeatureValue(json_variant, row, fieldMap.get(row));
      }

      preparedStatement.setString(3, json_variant.toString());

      // Set ingestion Id
      preparedStatement.setString(4, element.getIngestionId());

      // Set job Name
      preparedStatement.setString(5, jobName);

      preparedStatement.getConnection().commit();
    } catch (SQLException e) {
      log.error(
          String.format(
              "Could not construct prepared statement for JDBC IO. FeatureRow: %s:", element),
          e.getMessage());
    }
  }

  public static void setFeatureValue(JSONObject json_variant, String row, ValueProto.Value value) {
    ValueProto.Value.ValCase protoValueType = value.getValCase();
    try {
      switch (protoValueType) {
        case BYTES_VAL:
          json_variant.put(row, value.getBytesVal().toByteArray());
          break;
        case STRING_VAL:
          json_variant.put(row, value.getStringVal());
          break;
        case INT32_VAL:
          json_variant.put(row, value.getInt32Val());
          break;
        case INT64_VAL:
          json_variant.put(row, value.getInt64Val());
          break;
        case FLOAT_VAL:
          json_variant.put(row, value.getFloatVal());
          break;
        case DOUBLE_VAL:
          json_variant.put(row, value.getDoubleVal());
          break;
        case BOOL_VAL:
          json_variant.put(row, value.getBoolVal());
          break;
        case STRING_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getStringListVal().toByteArray()));
          break;
        case BYTES_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getBytesListVal().toByteArray()));
          break;
        case INT64_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getInt64ListVal().toByteArray()));
          break;
        case INT32_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getInt32ListVal().toByteArray()));
          break;
        case FLOAT_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getFloatListVal().toByteArray()));
          break;
        case DOUBLE_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getDoubleListVal().toByteArray()));
          break;
        case BOOL_LIST_VAL:
          json_variant.put(
              row, Base64.getEncoder().encodeToString(value.getBoolListVal().toByteArray()));
          break;
        case VAL_NOT_SET:
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Could not determine field protoValueType for incoming feature row: %s",
                  protoValueType));
      }
    } catch (IllegalArgumentException e) {
      log.error(
          String.format(
              "Could not cast value %s of type %s int SQL field: ",
              value.toString(), protoValueType),
          e.getMessage());
    }
  }

//@Override
//public String getFeatureRowInsertSql(FeatureSetSpec featureSetSpec) {
//	// TODO Auto-generated method stub
//	return null;
//}
}
