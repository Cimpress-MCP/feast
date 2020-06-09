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

import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.api.retriever.HistoricalRetriever;
import io.grpc.Status;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class JdbcHistoricalRetriever implements HistoricalRetriever {

  private final String password;
  private final String username;
  private final String url;
  private final String className;
  private final String stagingLocation;

  public Connection getConnection() {
    this.connect();
    return connection;
  }

  Connection connection;

  private JdbcHistoricalRetriever(Map<String, String> config) {
    this.className = config.getOrDefault("class_name", "");
    this.url = config.get("url");
    this.username = config.get("username");
    this.password = config.get("password");
    this.stagingLocation = config.get("staging_location");
    this.connect();
  }

  public static HistoricalRetriever create(Map<String, String> config) {
    return new JdbcHistoricalRetriever(config);
  }

  private void connect() {
    if (this.connection != null) {
      return;
    }
    try {
      Class.forName(className);
      // Username and password is provided
      if (!username.isEmpty() && !password.isEmpty()) {
        this.connection = DriverManager.getConnection(url);
      }
      // Only username provided
      if (!username.isEmpty()) {
        this.connection = DriverManager.getConnection(url, username, null);
      }
      this.connection = DriverManager.getConnection(url, username, password);
    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s", url, className),
          e);
    }
  }

  @Override
  public HistoricalRetrievalResult getHistoricalFeatures(
      String retrievalId,
      ServingAPIProto.DatasetSource datasetSource,
      List<FeatureSetRequest> featureSetRequests) {

    // Get connection to database
    Connection conn = this.getConnection();

    // 1. Ensure correct entity row file format
    ServingAPIProto.DataFormat dataFormat = datasetSource.getFileSource().getDataFormat();
    if (!dataFormat.equals(ServingAPIProto.DataFormat.DATA_FORMAT_CSV)) {
      throw new IllegalArgumentException(
          String.format(
              "Only CSV imports are allows for JDBC sources. Type provided was %s",
              dataFormat.name()));
    }

    // 2. Build feature set query infos
    List<FeatureSetQueryInfo> featureSetQueryInfos =
        QueryTemplater.getFeatureSetInfos(featureSetRequests);

    // 3. Load entity rows into database
    Iterator<String> fileList = datasetSource.getFileSource().getFileUrisList().iterator();
    String entityTableWithRowCountName = this.loadEntities(conn, featureSetQueryInfos, fileList);

    // 4. Retrieve the temporal bounds of the entity dataset provided
    Map<String, Timestamp> timestampLimits =
        this.getTimestampLimits(conn, entityTableWithRowCountName);

    // 5. Generate the subqueries
    List<String> featureSetQueries =
        this.generateQueries(entityTableWithRowCountName, timestampLimits, featureSetQueryInfos);

    // 6. Run the subqueries and collect outputs
    String resultTable =
        this.runBatchQuery(conn, entityTableWithRowCountName, featureSetQueryInfos, featureSetQueries);

    String fileUri = exportResultsToDisk(conn, resultTable, stagingLocation);
    List<String> fileUris = new ArrayList<>();
    fileUris.add(fileUri);
    return HistoricalRetrievalResult.success(
        retrievalId, fileUris, ServingAPIProto.DataFormat.DATA_FORMAT_AVRO);
  }

  private String exportResultsToDisk(Connection conn, String resultTable, String stagingLocation) {
    URI stagingUri;
    try {
      stagingUri = new URI(stagingLocation);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Could not parse staging location: %s", stagingLocation), e);
    }
    String stagingPath = stagingUri.getPath();
    String exportPath = String.format("%s/%s.csv", stagingPath.replaceAll("/$", ""), resultTable);
    String exportTableSqlQuery = null;
    try {
      Statement statement = conn.createStatement();
      exportTableSqlQuery =
          String.format(
              "COPY %s TO '%s' WITH (DELIMITER E'\t', FORMAT CSV, HEADER);",
              resultTable, exportPath);
      statement.executeUpdate(exportTableSqlQuery);
      return exportPath;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not export resulting historical dataset using query: \n%s",
              exportTableSqlQuery),
          e);
    }
  }

  private List<String> getEntityTableColumns(Connection conn, String entityTableName) {
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

  private List<String> generateQueries(
      String entityTableName,
      Map<String, Timestamp> timestampLimits,
      List<FeatureSetQueryInfo> featureSetQueryInfos) {
    List<String> featureSetQueries = new ArrayList<>();
    try {
      for (FeatureSetQueryInfo featureSetInfo : featureSetQueryInfos) {
        String query =
            QueryTemplater.createFeatureSetPointInTimeQuery(
                featureSetInfo,
                entityTableName,
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

  private String loadEntities(
      Connection conn, List<FeatureSetQueryInfo> featureSetQueryInfos, Iterator<String> fileList) {
    // Create table from existing feature set entities
    String entityTable = createStagedEntityTable(conn, featureSetQueryInfos);

    // Load files into database
    loadEntitiesFromFile(conn, entityTable, fileList);

    // Return entity table
    return entityTable;
  }

  private void loadEntitiesFromFile(Connection conn, String tableName, Iterator<String> fileList) {
    while (fileList.hasNext()) {

      File filePath;
      String fileString = fileList.next();
      try {
        filePath = new File(new URI(fileString));
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Could not parse file string %s", fileString), e);
      }

      Statement statement;
      String tempTableForLoad = createTempTableName();
      String loadEntitiesQuery =
          QueryTemplater.createLoadEntityQuery(tableName, tempTableForLoad, filePath);
      try {
        statement = conn.createStatement();
        statement.executeUpdate(loadEntitiesQuery);
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Could not load entity data from %s into table %s using query: \n%s",
                filePath, tableName, loadEntitiesQuery),
            e);
      }
    }
  }

  private String createStagedEntityTable(
      Connection conn, List<FeatureSetQueryInfo> featureSetQueryInfos) {
    String entityTableWithRowCountName = createTempTableName();
    String entityTableRowCountQuery =
        QueryTemplater.createEntityTableRowCountQuery(
            entityTableWithRowCountName, featureSetQueryInfos);
    Statement statement;
    try {
      statement = conn.createStatement();
      statement.executeUpdate(entityTableRowCountQuery);
      return entityTableWithRowCountName;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not create staged entity table with columns from feature sets %s.",
              featureSetQueryInfos.toString()),
          e);
    }
  }

  String runBatchQuery(
      Connection conn,
      String entityTableName,
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> featureSetQueries) {
    ExecutorService executorService = Executors.newFixedThreadPool(featureSetQueries.size());
    ExecutorCompletionService<FeatureSetQueryInfo> executorCompletionService =
        new ExecutorCompletionService<>(executorService);

    // For each of the feature sets requested, start a synchronous job joining the features in that
    // feature set to the provided entity table

    // TODO: This desperately needs optimization!
    for (int i = 0; i < featureSetQueries.size(); i++) {
      String featureSetTempTable = createTempTableName();
      String featureSetQuery = featureSetQueries.get(i);

      Statement statement;
      try {
        statement = conn.createStatement();
        statement.executeUpdate(
            String.format("CREATE TABLE \"%s\" AS (%s)", featureSetTempTable, featureSetQuery));
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

    List<String> entityTableColumnNames = getEntityTableColumns(conn, entityTableName);

    String joinQuery =
        QueryTemplater.createJoinQuery(
            featureSetQueryInfos, entityTableColumnNames, entityTableName);

    String resultTable = createTempTableName();

    Statement statement;
    try {
      statement = conn.createStatement();
      statement.executeUpdate(String.format("CREATE TABLE \"%s\" AS (%s)", resultTable, joinQuery));
      return resultTable;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Failed to create resulting combined point-in-time joined table.\nDestination table: %s\nQuery: %s",
              resultTable, joinQuery),
          e);
    }
  }

  private Map<String, Timestamp> getTimestampLimits(Connection conn, String entityTableName) {
    String timestampLimitSqlQuery = QueryTemplater.createTimestampLimitQuery(entityTableName);
    Map<String, Timestamp> timestampLimits = new HashMap<>();
    Statement statement;
    try {
      statement = conn.createStatement();
      ResultSet rs = statement.executeQuery(timestampLimitSqlQuery);

      while (rs.next()) {
        Timestamp min_ts = rs.getTimestamp("min"); // Get minimum timestamp
        Timestamp max_ts = rs.getTimestamp("max"); // Get maximum timestamp
        timestampLimits.putIfAbsent("min", min_ts);
        timestampLimits.putIfAbsent("max", max_ts);
      }
      return timestampLimits;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Could not query entity table %s for timestamp bounds.", entityTableName),
          e);
    }
  }

  public String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }

  @Override
  public String getStagingLocation() {
    return this.stagingLocation;
  }
}
