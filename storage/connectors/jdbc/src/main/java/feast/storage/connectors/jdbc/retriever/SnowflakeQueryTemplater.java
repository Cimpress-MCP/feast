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
package feast.storage.connectors.jdbc.retriever;

import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.storage.connectors.jdbc.connection.JdbcConnectionProvider;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.*;

public class SnowflakeQueryTemplater extends AbstractJdbcQueryTemplater {
  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME_SNOWFLAKE =
      "templates/single_featureset_pit_join_snowflake.sql";
  private static final String JOIN_TEMPLATE_NAME_SNOWFLAKE =
      "templates/join_featuresets_snowflake.sql";
  private static final String VARIANT_COLUMN_NAME = "feature";

  public SnowflakeQueryTemplater(JdbcConnectionProvider connectionProvider) {
    super(connectionProvider);
  }

  @Override
  protected List<String> createEntityTableRowCountQuery(
      String destinationTable, List<FeatureSetQueryInfo> featureSetQueryInfos) {
    StringJoiner featureSetTableSelectJoiner = new StringJoiner(", ");
    StringJoiner featureSetTableFromJoiner = new StringJoiner(" CROSS JOIN ");
    Set<String> entities = new HashSet<>();
    List<String> entityColumns = new ArrayList<>();
    for (FeatureSetQueryInfo featureSetQueryInfo : featureSetQueryInfos) {
      String table = featureSetQueryInfo.getFeatureSetTable();
      for (String entity : featureSetQueryInfo.getEntities()) {
        if (!entities.contains(entity)) {
          entities.add(entity);
          // parse entities from FEATURE variant column
          entityColumns.add(
              String.format("%s.%s:%s AS %s", table, VARIANT_COLUMN_NAME, entity, entity));
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
    createEntityTableRowCountQueries.add(
        String.format("ALTER TABLE %s ADD COLUMN event_timestamp TIMESTAMP;", destinationTable));
    return createEntityTableRowCountQueries;
  }

  // TODO: support filepath as s3 location
  @Override
  protected List<String> createLoadEntityQuery(
      String destinationTable, String stagingLocation, URI fileUri) {
    List<String> queries = new ArrayList<>();
    String csvFormatQuey =
        String.format(
            "create or replace file format CSV_format type = 'CSV' field_delimiter = ',' skip_header=1;");
    String createStageQuery =
        String.format(
            "create or replace stage my_s3_stage\n"
                + "  storage_integration = s3_int\n"
                + "  url = '%s';",
            stagingLocation);
    // TODO: fileUri must be the in the stagingLocation??
    String folderPath = fileUri.toString().substring(stagingLocation.length());
    String copyIntoDestTable =
        String.format(
            "COPY INTO %s FROM '@my_s3_stage/%s' FILE_FORMAT = CSV_format on_error = 'skip_file';",
            destinationTable, folderPath);

    String addRowNum =
        String.format(
            "CREATE OR REPLACE TABLE %s as SELECT *, ROW_NUMBER() OVER (ORDER BY 1) AS row_number FROM %s;",
            destinationTable, destinationTable);
    String[] queryArray =
        new String[] {csvFormatQuey, createStageQuery, copyIntoDestTable, addRowNum};
    queries.addAll(Arrays.asList(queryArray));
    return queries;
  }

  @Override
  protected String createFeatureSetPointInTimeQuery(
      FeatureSetQueryInfo featureSetInfo,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException {
    PebbleTemplate template;
    template = engine.getTemplate(FEATURESET_TEMPLATE_NAME_SNOWFLAKE);

    Map<String, Object> context = new HashMap<>();
    context.put("variantColumn", VARIANT_COLUMN_NAME);
    context.put("featureSet", featureSetInfo);

    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  @Override
  protected String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName) {

    PebbleTemplate template;
    template = engine.getTemplate(JOIN_TEMPLATE_NAME_SNOWFLAKE);
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

  @Override
  protected List<String> generateExportTableSqlQuery(String resultTable, String stagingPath) {

    List<String> exportTableSqlQueries = new ArrayList<>();
    // TODO: s3 integration needs to be configurable: s3_int
    String createStageQuery =
        String.format(
            "create or replace stage my_s3_stage\n"
                + "  storage_integration = s3_int\n"
                + "  url = '%s';",
            stagingPath);
    String copyIntoStageQuery =
        String.format(
            "COPY INTO '@my_s3_stage/%s.%s' FROM %s file_format = (type=csv compression='gzip')\n"
                + "single=true header = true;",
            resultTable, EXPORT_FILE_FORMAT, resultTable);
    String[] queryArray = new String[] {createStageQuery, copyIntoStageQuery};
    exportTableSqlQueries.addAll(Arrays.asList(queryArray));
    return exportTableSqlQueries;
  }
}
