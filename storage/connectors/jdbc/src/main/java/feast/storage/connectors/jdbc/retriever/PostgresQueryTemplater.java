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
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresQueryTemplater extends AbstractJdbcQueryTemplater {
  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME_POSTGRES =
      "templates/single_featureset_pit_join_postgres.sql";
  private static final String JOIN_TEMPLATE_NAME_POSTGRES =
      "templates/join_featuresets_postgres.sql";

  public PostgresQueryTemplater(JdbcConnectionProvider connectionProvider) {
    super(connectionProvider);
  }

  @Override
  protected List<String> createLoadEntityQuery(
      String destinationTable, String temporaryTable, File filePath) {
    List<String> queries = new ArrayList<>();
    queries.add(
        String.format("CREATE TABLE %s AS (SELECT * FROM %s);", temporaryTable, destinationTable));
    //      queries.add(String.format("ALTER TABLE %s DROP COLUMN row_number;",temporaryTable));
    queries.add(
        String.format("COPY %s FROM '%s' DELIMITER ',' CSV HEADER;", temporaryTable, filePath));
    queries.add(
        String.format("INSERT INTO %s SELECT * FROM %s;", destinationTable, temporaryTable));
    queries.add(String.format("DROP TABLE %s;", temporaryTable));
    queries.add(
        String.format("ALTER TABLE \"%s\" ADD COLUMN row_number SERIAL;", destinationTable));
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
    template = engine.getTemplate(FEATURESET_TEMPLATE_NAME_POSTGRES);

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

  @Override
  protected String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName) {

    PebbleTemplate template;
    template = engine.getTemplate(JOIN_TEMPLATE_NAME_POSTGRES);
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
    String exportPath = String.format("%s/%s.csv", stagingPath.replaceAll("/$", ""), resultTable);
    List<String> exportTableSqlQueries = new ArrayList<>();
    exportTableSqlQueries.add(
        String.format(
            "COPY %s TO '%s' WITH (DELIMITER E'\t', FORMAT CSV, HEADER);",
            resultTable, exportPath));
    return exportTableSqlQueries;
  }
}
