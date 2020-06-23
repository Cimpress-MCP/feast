/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.storage.connectors.jdbc.retriever;

import com.google.protobuf.Duration;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.storage.api.retriever.FeatureSetRequest;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

public class QueryTemplater {

  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME_POSTGRES =
      "templates/single_featureset_pit_join_postgres.sql";
  private static final String JOIN_TEMPLATE_NAME_POSTGRES =
      "templates/join_featuresets_postgres.sql";
  private static final String FEATURESET_TEMPLATE_NAME_SNOWFLAKE =
      "templates/single_featureset_pit_join_snowflake.sql";
  private static final String JOIN_TEMPLATE_NAME_SNOWFLAKE =
      "templates/join_featuresets_snowflake.sql";

  /**
   * Get the query for retrieving the earliest and latest timestamps in the entity dataset.
   *
   * @param leftTableName full entity dataset name
   * @return timestamp limit BQ SQL query
   */
  public static String createTimestampLimitQuery(String leftTableName) {
    return String.format(
        "SELECT max(event_timestamp) as max, min(event_timestamp) as min from %s", leftTableName);
  }

  public static List<String> createEntityTableRowCountQuery(
      String className, String destinationTable, List<FeatureSetQueryInfo> featureSetQueryInfos) {
    StringJoiner featureSetTableSelectJoiner = new StringJoiner(", ");
    StringJoiner featureSetTableFromJoiner = new StringJoiner(" CROSS JOIN ");
    Set<String> entities = new HashSet<>();
    List<String> entityColumns = new ArrayList<>();
    for (FeatureSetQueryInfo featureSetQueryInfo : featureSetQueryInfos) {
      String table = featureSetQueryInfo.getFeatureSetTable();
      for (String entity : featureSetQueryInfo.getEntities()) {
        if (!entities.contains(entity)) {
          entities.add(entity);
          entityColumns.add(String.format("%s.%s", table, entity));
        }
      }
      featureSetTableFromJoiner.add(table);
    }
    // Must preserve alphabetical order because column mapping isn't supported in COPY loads of CSV
    entityColumns.sort(Comparator.comparing(entity -> entity.split("\\.")[0]));
    entityColumns.forEach(featureSetTableSelectJoiner::add);

    List<String> createEntityTableRowCountQueries = new ArrayList<>();
    createEntityTableRowCountQueries.add(
        String.format(
            "CREATE TABLE %s AS (SELECT %s FROM %s WHERE 1 = 2);",
            destinationTable, featureSetTableSelectJoiner, featureSetTableFromJoiner));
    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
      // 1st create a sequence
      //      createEntityTableRowCountQueries.add("create or replace sequence row_seq start = 1
      // increment = 1;");
      // 2nd insert sequence as the row num
      createEntityTableRowCountQueries.add(
          String.format("ALTER TABLE %s ADD COLUMN event_timestamp TIMESTAMP;", destinationTable));
      //      createEntityTableRowCountQueries.add(
      //              String.format(
      //                      "ALTER TABLE \"%s\" ADD COLUMN row_number INT DEFAULT
      // row_seq.nextval;", destinationTable));
    } else {
      createEntityTableRowCountQueries.add(
          String.format(
              "ALTER TABLE \"%s\" ADD COLUMN event_timestamp TIMESTAMP;", destinationTable));
      //      createEntityTableRowCountQueries.add(
      //              String.format(
      //                      "ALTER TABLE \"%s\" ADD COLUMN row_number SERIAL;",
      // destinationTable));
    }

    return createEntityTableRowCountQueries;
  }

  /**
   * Generate the information necessary for the sql templating for point in time correctness join to
   * the entity dataset for each feature set requested.
   *
   * @param featureSetRequests List of {@link FeatureSetRequest} containing a {@link FeatureSetSpec}
   *     and its corresponding {@link FeatureReference}s provided by the user.
   * @return List of FeatureSetInfos
   */
  public static List<FeatureSetQueryInfo> getFeatureSetInfos(
      List<FeatureSetRequest> featureSetRequests) throws IllegalArgumentException {

    List<FeatureSetQueryInfo> featureSetInfos = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      FeatureSetSpec spec = featureSetRequest.getSpec();
      Duration maxAge = spec.getMaxAge();
      List<String> fsEntities =
          spec.getEntitiesList().stream().map(EntitySpec::getName).collect(Collectors.toList());
      List<FeatureReference> features = featureSetRequest.getFeatureReferences().asList();
      featureSetInfos.add(
          new FeatureSetQueryInfo(
              spec.getProject(), spec.getName(), maxAge.getSeconds(), fsEntities, features));
    }
    return featureSetInfos;
  }

  /**
   * Generate the query for point in time correctness join of data for a single feature set to the
   * entity dataset.
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param leftTableName entity dataset name
   * @param minTimestamp earliest allowed timestamp for the historical data in feast
   * @param maxTimestamp latest allowed timestamp for the historical data in feast
   * @return point in time correctness join BQ SQL query
   */
  public static String createFeatureSetPointInTimeQuery(
      String className,
      FeatureSetQueryInfo featureSetInfo,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {
    PebbleTemplate template;
    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
      template = engine.getTemplate(FEATURESET_TEMPLATE_NAME_SNOWFLAKE);
    } else {
      template = engine.getTemplate(FEATURESET_TEMPLATE_NAME_POSTGRES);
    }
    Map<String, Object> context = new HashMap<>();
    context.put("featureSet", featureSetInfo);

    // TODO: Subtract max age to min timestamp
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * @param featureSetInfos List of FeatureSetInfos containing information about the feature set
   *     necessary for the query templating
   * @param entityTableColumnNames list of column names in entity table
   * @param leftTableName entity dataset name
   * @return query to join temporary feature set tables to the entity table
   */
  public static String createJoinQuery(
      String className,
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName) {

    PebbleTemplate template;
    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
      template = engine.getTemplate(JOIN_TEMPLATE_NAME_SNOWFLAKE);
    } else {
      template = engine.getTemplate(JOIN_TEMPLATE_NAME_POSTGRES);
    }
    Map<String, Object> context = new HashMap<>();
    context.put("entities", entityTableColumnNames);
    context.put("featureSets", featureSetInfos);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    try {
      template.evaluate(writer, context);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Could not successfully template a join query to produce the final point-in-time result table. \nContext: %s",
              context),
          e);
    }
    return writer.toString();
  }

  public static List<String> createLoadEntityQuery(
      String className, String destinationTable, String temporaryTable, File filePath) {
    List<String> queries = new ArrayList<>();
    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
      queries.add(
          String.format(
              "CREATE TABLE %s AS (SELECT * FROM %s);", temporaryTable, destinationTable));
      queries.add(
          String.format(
              "create or replace file format CSV_format type = 'CSV' field_delimiter = ',' skip_header=1;"));
      queries.add(String.format("create or replace stage my_stage file_format = CSV_format;"));
      queries.add(String.format("put file://%s @my_stage auto_compress=false;", filePath));
      // TODO: generic staging snowflake_proj_entity_rows.csv
      queries.add(
          String.format(
              "COPY INTO %s FROM '@my_stage/snowflake_proj_entity_rows.csv' FILE_FORMAT = CSV_format on_error = 'skip_file';",
              temporaryTable));
      queries.add(
          String.format("INSERT INTO %s SELECT * FROM %s;", destinationTable, temporaryTable));

      queries.add(String.format("DROP TABLE %s;", temporaryTable));
      queries.add(
          String.format(
              "CREATE OR REPLACE TABLE %s as SELECT *, ROW_NUMBER() OVER (ORDER BY 1) AS row_number FROM %s;",
              destinationTable, destinationTable));

    } else {
      queries.add(
          String.format(
              "CREATE TABLE %s AS (SELECT * FROM %s);", temporaryTable, destinationTable));
      //      queries.add(String.format("ALTER TABLE %s DROP COLUMN row_number;",temporaryTable));
      queries.add(
          String.format("COPY %s FROM '%s' DELIMITER ',' CSV HEADER;", temporaryTable, filePath));
      queries.add(
          String.format("INSERT INTO %s SELECT * FROM %s;", destinationTable, temporaryTable));
      queries.add(String.format("DROP TABLE %s;", temporaryTable));
      queries.add(
          String.format("ALTER TABLE \"%s\" ADD COLUMN row_number SERIAL;", destinationTable));
    }
    return queries;
  }
}
