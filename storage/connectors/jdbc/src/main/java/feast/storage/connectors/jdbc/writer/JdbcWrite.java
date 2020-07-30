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

import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.SnowflakeConfig;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;

/**
 * A {@link PTransform} that writes {@link FeatureRowProto FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output successful writes, we cannot emit those, and so no success metrics will be
 * captured if this sink is used.
 */
public class JdbcWrite extends PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcWrite.class);
  private final JdbcTemplater jdbcTemplater;
  private final StoreProto.Store.SnowflakeConfig config;

  public JdbcWrite(SnowflakeConfig config, JdbcTemplater jdbcTemplater) {

    this.config = config;
    this.jdbcTemplater = jdbcTemplater;
  }

  public StoreProto.Store.SnowflakeConfig getConfig() {
    return config;
  }

  public JdbcTemplater getJdbcTemplater() {
    return jdbcTemplater;
  }

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {

    String jobName = input.getPipeline().getOptions().getJobName();

    int batchSize = this.config.getBatchSize() > 0 ? config.getBatchSize() : 1;

    PCollection<FeatureRow> feature = input;

    feature.apply(
        String.format("WriteFeatureRowToJdbcIO-%s", jobName),
        JdbcIO.<FeatureRowProto.FeatureRow>write()
            .withDataSourceConfiguration(create_dsconfig(this.config))
            .withStatement(jdbcTemplater.getFeatureRowInsertSql(this.config.getTableName()))
            .withBatchSize(batchSize)
            .withPreparedStatementSetter(
                new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
                  @Override
                  public void setParameters(
                      FeatureRowProto.FeatureRow element, PreparedStatement preparedStatement) {
                    try {

                      Map<String, ValueProto.Value> fieldMap =
                          element.getFieldsList().stream()
                              .collect(
                                  Collectors.toMap(
                                      FieldProto.Field::getName, FieldProto.Field::getValue));

                      // Set event_timestamp
                      Instant eventTsInstant =
                          Instant.ofEpochSecond(element.getEventTimestamp().getSeconds())
                              .plusNanos(element.getEventTimestamp().getNanos());

                      preparedStatement.setTimestamp(
                          1,
                          Timestamp.from(eventTsInstant),
                          Calendar.getInstance(TimeZone.getTimeZone("UTC")));

                      // Set created_timestamp
                      preparedStatement.setTimestamp(
                          2,
                          new Timestamp(System.currentTimeMillis()),
                          Calendar.getInstance(TimeZone.getTimeZone("UTC")));

                      // Set Project

                      preparedStatement.setString(3, getProject(element.getFeatureSet()));

                      // Set FeatureSet

                      preparedStatement.setString(4, getFeatureSet(element.getFeatureSet()));

                      //
                      JSONObject json_variant = new JSONObject();
                      for (String row : fieldMap.keySet()) {
                        setFeatureValue(json_variant, row, fieldMap.get(row));
                      }

                      preparedStatement.setString(5, json_variant.toString());

                      // Set ingestion Id
                      preparedStatement.setString(6, element.getIngestionId());

                      // Set job Name
                      preparedStatement.setString(7, jobName);

                      preparedStatement.getConnection().commit();
                    } catch (SQLException e) {
                      log.error(
                          String.format(
                              "Could not construct prepared statement for JDBC IO. FeatureRow: %s:",
                              element),
                          e.getMessage());
                    }
                  }
                }));

    PCollection<FeatureRowProto.FeatureRow> successfulInserts =
        input.apply(
            "dummy",
            ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {}
                }));

    PCollection<FailedElement> failedElements =
        input.apply(
            "dummyFailed",
            ParDo.of(
                new DoFn<FeatureRowProto.FeatureRow, FailedElement>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {}
                }));

    return WriteResult.in(input.getPipeline(), successfulInserts, failedElements);
  }

  public String getProject(String featureSet) {

    return featureSet.split("/")[0];
  }

  public String getFeatureSet(String featureSet) {

    return featureSet.split("/")[1];
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

  public static JdbcIO.DataSourceConfiguration create_dsconfig(
      StoreProto.Store.SnowflakeConfig jdbcConfig) {
    String username = jdbcConfig.getUsername();
    String password = jdbcConfig.getPassword();
    String className = jdbcConfig.getClassName();
    String url = jdbcConfig.getUrl();
    log.info("setting the jdbc connection");
    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
      String database = jdbcConfig.getDatabase();
      String schema = jdbcConfig.getSchema();
      String warehouse = jdbcConfig.getWarehouse();
      return JdbcIO.DataSourceConfiguration.create(className, url)
          .withUsername(!username.isEmpty() ? username : null)
          .withPassword(!password.isEmpty() ? password : null)
          .withConnectionProperties(
              String.format("warehouse=%s;db=%s;schema=%s", warehouse, database, schema));

    } else {
      return JdbcIO.DataSourceConfiguration.create(className, url)
          .withUsername(!username.isEmpty() ? username : null)
          .withPassword(!password.isEmpty() ? password : null);
    }
  }
}
