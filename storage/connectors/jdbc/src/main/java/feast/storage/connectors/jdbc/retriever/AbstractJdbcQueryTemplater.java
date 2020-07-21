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
import feast.proto.core.FeatureSetProto;
import feast.proto.serving.ServingAPIProto;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.connectors.jdbc.connection.JdbcConnectionProvider;
import io.grpc.Status;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractJdbcQueryTemplater implements JdbcQueryTemplater {
  protected static final String EXPORT_FILE_FORMAT = "csv.gz";
  private Connection connection;

  public AbstractJdbcQueryTemplater(
      Map<String, String> databaseConfig, JdbcConnectionProvider connectionProvider) {
    this.connection = connectionProvider.getConnection();
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
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> entitySourceUris,
      String stagingUri) {
    // Create table from existing feature set entities
    String entityTable = this.createStagedEntityTable(featureSetQueryInfos);

    // Load files into database
    this.loadEntitiesFromFile(entityTable, entitySourceUris);

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

  // TODO: use stagingUri
  @Override
  public String exportResultTableToStagingLocation(String resultTable, String stagingUri) {

    String exportPath = String.format("%s%s.%s", stagingUri, resultTable, EXPORT_FILE_FORMAT);
    List<String> exportTableSqlQueries = this.generateExportTableSqlQuery(resultTable, stagingUri);
    try {
      Statement statement = this.connection.createStatement();
      for (String query : exportTableSqlQueries) {
        statement.execute(query);
      }
      return exportPath;
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not export resulting historical dataset with data format: \n%s \n using query: \n%s",
              EXPORT_FILE_FORMAT, exportTableSqlQueries),
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

  /**
   * @param destinationTable
   * @param featureSetQueryInfos
   * @return
   */
  protected abstract List<String> createEntityTableRowCountQuery(
      String destinationTable, List<FeatureSetQueryInfo> featureSetQueryInfos);

  protected void loadEntitiesFromFile(String entityTable, List<String> entitySourceUris) {
    for (String entitySourceUri : entitySourceUris) {
      Statement statement;
      List<String> loadEntitiesQueries = this.createLoadEntityQuery(entityTable, entitySourceUri);
      try {
        statement = this.connection.createStatement();
        for (String query : loadEntitiesQueries) {
          statement.execute(query);
        }
      } catch (SQLException e) {
        throw new RuntimeException(
            String.format(
                "Could not load entity data from %s into table %s using query: \n%s",
                entitySourceUri, entityTable, loadEntitiesQueries),
            e);
      }
    }
  }
  /**
   * Load entity rows from entitySourceUri to the destinationTable
   *
   * @param destinationTable the entity table
   * @param entitySourceUri a csv file uri contains entity rows, with columns: entity_id and
   *     event_timestamp. eg: a S3 bucket or GCP location
   * @return
   */
  protected abstract List<String> createLoadEntityQuery(
      String destinationTable, String entitySourceUri);

  /**
   * Generate the query for point in time correctness join of data for a single feature set to the
   * entity dataset.
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param leftTableName entityTableWithRowCountName: entity table name
   * @param minTimestamp earliest allowed timestamp for the historical data in feast
   * @param maxTimestamp latest allowed timestamp for the historical data in feast
   * @return point in time correctness join BQ SQL query
   */
  protected abstract String createFeatureSetPointInTimeQuery(
      FeatureSetQueryInfo featureSetInfo,
      String leftTableName,
      String minTimestamp,
      String maxTimestamp)
      throws IOException;

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
  protected abstract String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName);

  /**
   * Generate the SQL queries to Export the result table from database to the staging location with
   * name "{exportPath}/{resultTable}.csv"
   *
   * @param resultTable the table in the database, needs to exported
   * @param stagingUri staging uri, eg: a S3 bucket or GCP location
   * @return a list of sql queries for exporting
   */
  protected abstract List<String> generateExportTableSqlQuery(
      String resultTable, String stagingUri);
}
