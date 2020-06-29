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

import com.google.protobuf.Duration;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.connectors.jdbc.connection.JdbcConnectionProvider;
import io.grpc.Status;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractJdbcQueryTemplater implements JdbcQueryTemplater {
  private Connection connection;
  private static final PebbleEngine engine = new PebbleEngine.Builder().build();
  private static final String FEATURESET_TEMPLATE_NAME_POSTGRES =
      "templates/single_featureset_pit_join_postgres.sql";
  private static final String JOIN_TEMPLATE_NAME_POSTGRES =
      "templates/join_featuresets_postgres.sql";

  public AbstractJdbcQueryTemplater(JdbcConnectionProvider connectionProvider) {
    this.connection = connectionProvider.getConnection();
  }

  protected Connection getConnection() {
    return this.connection;
  }

  protected String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }

  @Override
  public List<FeatureSetQueryInfo> getFeatureSetInfos(List<FeatureSetRequest> featureSetRequests)
      throws IllegalArgumentException {
    List<FeatureSetQueryInfo> featureSetInfos = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      FeatureSetProto.FeatureSetSpec spec = featureSetRequest.getSpec();
      Duration maxAge = spec.getMaxAge();
      List<String> fsEntities =
          spec.getEntitiesList().stream()
              .map(FeatureSetProto.EntitySpec::getName)
              .collect(Collectors.toList());
      List<ServingAPIProto.FeatureReference> features =
          featureSetRequest.getFeatureReferences().asList();
      featureSetInfos.add(
          new FeatureSetQueryInfo(
              spec.getProject(), spec.getName(), maxAge.getSeconds(), fsEntities, features));
    }
    return featureSetInfos;
  }

  @Override
  public String loadEntities(
      List<FeatureSetQueryInfo> featureSetQueryInfos, Iterator<String> fileList) {
    // Create table from existing feature set entities
    String entityTable = this.createStagedEntityTable(featureSetQueryInfos);

    // Load files into database
    this.loadEntitiesFromFile(entityTable, fileList);

    // Return entity table
    return entityTable;
  }

  @Override
  public Map<String, Timestamp> getTimestampLimits(String entityTableWithRowCountName) {
    String timestampLimitSqlQuery = this.createTimestampLimitQuery(entityTableWithRowCountName);
    Map<String, Timestamp> timestampLimits = new HashMap<>();
    Statement statement;
    try {
      statement = this.connection.createStatement();
      ResultSet rs = statement.executeQuery(timestampLimitSqlQuery);

      while (rs.next()) {
        Timestamp min_ts = rs.getTimestamp("MIN"); // Get minimum timestamp
        Timestamp max_ts = rs.getTimestamp("MAX"); // Get maximum timestamp
        timestampLimits.putIfAbsent("min", min_ts);
        timestampLimits.putIfAbsent("max", max_ts);
      }
      return timestampLimits;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not query entity table %s for timestamp bounds.", entityTableWithRowCountName),
          e);
    }
  }

  @Override
  public List<String> generateFeatureSetQueries(
      String entityTableWithRowCountName,
      Map<String, Timestamp> timestampLimits,
      List<FeatureSetQueryInfo> featureSetQueryInfos) {
    List<String> featureSetQueries = new ArrayList<>();
    try {
      for (FeatureSetQueryInfo featureSetInfo : featureSetQueryInfos) {
        String query =
            this.createFeatureSetPointInTimeQuery(
                featureSetInfo,
                entityTableWithRowCountName,
                timestampLimits.get("min").toString(),
                timestampLimits.get("max").toString());
        featureSetQueries.add(query);
      }
    } catch (IOException e) {
      throw Status.INTERNAL
          .withDescription("Unable to generate query for batch retrieval")
          .withCause(e)
          .asRuntimeException();
    }
    return featureSetQueries;
  }

  @Override
  public String runBatchQuery(
      String entityTableName,
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> featureSetQueries) {
    // For each of the feature sets requested, start a synchronous job joining the features in that
    // feature set to the provided entity table

    // TODO: This needs optimization!
    for (int i = 0; i < featureSetQueries.size(); i++) {
      String featureSetTempTable = createTempTableName();
      String featureSetQuery = featureSetQueries.get(i);

      Statement statement;
      try {
        statement = this.connection.createStatement();
        String query =
            String.format("CREATE TABLE %s AS (%s)", featureSetTempTable, featureSetQuery);
        statement.executeUpdate(query);
        featureSetQueryInfos.get(i).setJoinedTable(featureSetTempTable);
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Could not create single staged point in time table for feature set: %s.",
                featureSetQueryInfos.get(i).getName()),
            e);
      }
    }
    // Generate and run a join query to collect the outputs of all the
    // subqueries into a single table.
    List<String> entityTableColumnNames =
        this.getEntityTableColumns(this.connection, entityTableName);

    String joinQuery =
        this.createJoinQuery(featureSetQueryInfos, entityTableColumnNames, entityTableName);

    String resultTable = createTempTableName();

    Statement statement;
    try {
      statement = this.connection.createStatement();
      String resultTableQuery = String.format("CREATE TABLE %s AS (%s)", resultTable, joinQuery);
      statement.executeUpdate(resultTableQuery);
      return resultTable;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to create resulting combined point-in-time joined table.\nDestination table: %s\nQuery: %s",
              resultTable, joinQuery),
          e);
    }
  }

  @Override
  public String exportResultTableToStagingLocation(String resultTable, String stagingLocation) {
    URI stagingUri;
    try {
      stagingUri = new URI(stagingLocation);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Could not parse staging location: %s", stagingLocation), e);
    }
    String stagingPath = stagingUri.getPath();
    String exportPath = String.format("%s/%s.csv", stagingPath.replaceAll("/$", ""), resultTable);
    List<String> exportTableSqlQueries = this.generateExportTableSqlQuery(resultTable, stagingPath);
    try {
      Statement statement = this.connection.createStatement();
      for (String query : exportTableSqlQueries) {
        statement.execute(query);
      }
      return exportPath;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not export resulting historical dataset using query: \n%s",
              exportTableSqlQueries),
          e);
    }
  }

  /*
   *  Helper methods: override in subclasses if necessary
   */

  protected String createStagedEntityTable(List<FeatureSetQueryInfo> featureSetQueryInfos) {
    String entityTableWithRowCountName = createTempTableName();
    List<String> entityTableRowCountQueries =
        this.createEntityTableRowCountQuery(entityTableWithRowCountName, featureSetQueryInfos);
    Statement statement;
    try {
      statement = this.connection.createStatement();
      for (String query : entityTableRowCountQueries) {
        statement.executeUpdate(query);
      }
      return entityTableWithRowCountName;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not create staged entity table with columns from feature sets %s.",
              featureSetQueryInfos.toString()),
          e);
    }
  }
  /**
   * Get the query for retrieving the earliest and latest timestamps in the entity dataset.
   *
   * @param leftTableName full entity dataset name
   * @return timestamp limit BQ SQL query
   */
  protected String createTimestampLimitQuery(String leftTableName) {
    return String.format(
        "SELECT max(event_timestamp) as MAX, min(event_timestamp) as MIN from %s", leftTableName);
  }

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
    createEntityTableRowCountQueries.add(
        String.format("ALTER TABLE %s ADD COLUMN event_timestamp TIMESTAMP;", destinationTable));
    return createEntityTableRowCountQueries;
  }

  protected void loadEntitiesFromFile(String entityTable, Iterator<String> fileList) {
    while (fileList.hasNext()) {

      File filePath;
      String fileString = fileList.next();
      try {
        URI fileURI = new URI(fileString);
        filePath = new File(fileString);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Could not parse file string %s", fileString), e);
      }

      Statement statement;
      String tempTableForLoad = createTempTableName();
      List<String> loadEntitiesQueries =
          this.createLoadEntityQuery(entityTable, tempTableForLoad, filePath);

      try {
        statement = this.connection.createStatement();
        for (String query : loadEntitiesQueries) {
          statement.execute(query);
        }
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Could not load entity data from %s into table %s using query: \n%s",
                filePath, entityTable, loadEntitiesQueries),
            e);
      }
    }
  }
  /**
   * Load entity rows from filePath to the destinationTable
   *
   * @param destinationTable
   * @param temporaryTable temporary table for staging
   * @param filePath csv file contains entity rows, with columns: entity_id and created_timestamp
   * @return
   */
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

  protected List<String> getEntityTableColumns(Connection conn, String entityTableName) {
    List<String> entityTableColumns = new ArrayList<>();
    try {
      Statement st = conn.createStatement();
      ResultSet rs =
          st.executeQuery(String.format("SELECT * FROM %s WHERE 1 = 0", entityTableName));
      ResultSetMetaData rsmd = rs.getMetaData();
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        String column = rsmd.getColumnName(i);
        if ("event_timestamp".equals(column) || "row_number".equals(column)) {
          continue;
        }
        entityTableColumns.add(column);
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Could not determine columns for table %s", entityTableName), e);
    }

    return entityTableColumns;
  }
  /**
   * @param featureSetInfos List of FeatureSetInfos containing information about the feature set
   *     necessary for the query templating
   * @param entityTableColumnNames list of column names in entity table
   * @param leftTableName entity dataset name
   * @return query to join temporary feature set tables to the entity table
   */
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
  /**
   * Generate the SQL queries to Export the result table from database to the staging location with
   * name "{exportPath}/{resultTable}.csv"
   *
   * @param resultTable the table in the database, needs to exported
   * @param stagingPath staging location
   * @return a list of sql queries for exporting
   */
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
