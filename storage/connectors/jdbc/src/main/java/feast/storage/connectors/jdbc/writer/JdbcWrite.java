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
import feast.proto.types.FeatureRowProto;
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
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;

/**
 * A {@link PTransform} that writes {@link FeatureRowProto FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output successful writes, we cannot emit those, and so no success metrics will be
 * captured if this sink is used.
 */
public class JdbcWrite extends PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcWrite.class);
  private final Map<String, FeatureSetProto.FeatureSet> subscribedFeatureSets;
  private final JdbcTemplater jdbcTemplater;
  private final StoreProto.Store.JdbcConfig config;

  public JdbcWrite(
      StoreProto.Store.JdbcConfig config,
      JdbcTemplater jdbcTemplater,
      Map<String, FeatureSetProto.FeatureSet> subscribedFeatureSets) {
    this.config = config;
    this.jdbcTemplater = jdbcTemplater;
    this.subscribedFeatureSets = subscribedFeatureSets;
    System.out.println("Inside class JDBC writer----");
    Map<String, String> sqlInsertStatements = new HashMap<>();
    for (String subscribedFeatureSetRef : subscribedFeatureSets.keySet()) {
      FeatureSetProto.FeatureSet subscribedFeatureSet =
          subscribedFeatureSets.get(subscribedFeatureSetRef);
      FeatureSetProto.FeatureSetSpec subscribedFeatureSetSpec = subscribedFeatureSet.getSpec();
      String featureRowInsertSql = jdbcTemplater.getFeatureRowInsertSql(subscribedFeatureSetSpec);
      sqlInsertStatements.put(subscribedFeatureSetRef, featureRowInsertSql);
    }
  }

  public StoreProto.Store.JdbcConfig getConfig() {
    return config;
  }

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();

    // Create a map of feature set references to incrementing partition numbers. This map will split
    // partition
    // The incoming feature rows an allow each of them to have a different INSERT statement based on
    // their feature set
    Map<String, Integer> featureSetToPartitionMap = new HashMap<>();

    PCollectionList<FeatureRowProto.FeatureRow> partitionedInput =
        applyPartitioningToPCollectionBasedOnFeatureSet(input, featureSetToPartitionMap);

    for (String featureSetRef : subscribedFeatureSets.keySet()) {
      // For this feature set reference find its partition number
      int partitionNumber = featureSetToPartitionMap.get(featureSetRef);

      // Find the PCollection that is associated with a partition number
      PCollection<FeatureRowProto.FeatureRow> featureSetInput =
          partitionedInput.get(partitionNumber);

      // Find the feature set spec associated with this partition
      FeatureSetProto.FeatureSetSpec currentFeatureSetSpec =
          subscribedFeatureSets.get(featureSetRef).getSpec();
      StoreProto.Store.JdbcConfig jdbcConfig = this.getConfig();

      // Apply the WriteFeatureRow transformation to this feature set partitioned input
      applyWriteFeatureRowToJdbcIo(
          jobName,
          featureSetRef,
          featureSetInput,
          currentFeatureSetSpec,
          jdbcConfig,
          jdbcTemplater);
    }

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

  private PCollectionList<FeatureRowProto.FeatureRow>
      applyPartitioningToPCollectionBasedOnFeatureSet(
          PCollection<FeatureRowProto.FeatureRow> input,
          Map<String, Integer> featureSetToPartitionMap) {

    // For each subscribed feature set, create a map relation between its feature set reference and
    // partition number
    for (String featureSetRef : this.subscribedFeatureSets.keySet()) {
      featureSetToPartitionMap.putIfAbsent(featureSetRef, featureSetToPartitionMap.size());
    }

    // Fork all incoming feature rows into partitions based on their feature set reference
    return input.apply(
        Partition.of(
            featureSetToPartitionMap.size(),
            new Partition.PartitionFn<FeatureRowProto.FeatureRow>() {
              @Override
              public int partitionFor(FeatureRowProto.FeatureRow featureRow, int numPartitions) {
                return featureSetToPartitionMap.get(featureRow.getFeatureSet());
              }
            }));
  }

  private static void applyWriteFeatureRowToJdbcIo(
      String jobName,
      String featureSetRef,
      PCollection<FeatureRowProto.FeatureRow> featureSetInput,
      FeatureSetProto.FeatureSetSpec currentFeatureSetSpec,
      StoreProto.Store.JdbcConfig jdbcConfig,
      JdbcTemplater jdbcTemplater) {
    String username = jdbcConfig.getUsername();
    String password = jdbcConfig.getPassword();
    String className = jdbcConfig.getClassName();
    String url = jdbcConfig.getUrl();
    int batchSize = jdbcConfig.getBatchSize() > 0 ? jdbcConfig.getBatchSize() : 1;

    featureSetInput.apply(
        String.format("WriteFeatureRowToJdbcIO-%s", featureSetRef),
        JdbcIO.<FeatureRowProto.FeatureRow>write()
            .withDataSourceConfiguration(
                JdbcIO.DataSourceConfiguration.create(className, url)
                    .withUsername(!username.isEmpty() ? username : null)
                    .withPassword(!password.isEmpty() ? password : null))
            .withStatement(jdbcTemplater.getFeatureRowInsertSql(currentFeatureSetSpec))
            .withBatchSize(batchSize)
            .withPreparedStatementSetter(
                new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
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

                      // entities
                      int counter = 3;
                      for (FeatureSetProto.EntitySpec entitySpec :
                          currentFeatureSetSpec.getEntitiesList()) {
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
                      for (FeatureSetProto.FeatureSpec featureSpec :
                          currentFeatureSetSpec.getFeaturesList()) {
                        ValueProto.Value value =
                            fieldMap.getOrDefault(
                                featureSpec.getName(), ValueProto.Value.getDefaultInstance());
                        setPreparedStatementValue(preparedStatement, value, counter);
                        counter++;
                      }
                      preparedStatement.getConnection().commit();
                      System.out.println(preparedStatement);
                    } catch (SQLException e) {
                      log.error(
                          String.format(
                              "Could not construct prepared statement for JDBC IO. FeatureRow: %s:",
                              element),
                          e.getMessage());
                    }
                  }
                }));
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
