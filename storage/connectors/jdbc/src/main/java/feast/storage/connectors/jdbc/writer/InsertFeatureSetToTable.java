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

import com.google.api.services.bigquery.model.TableSchema;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.proto.types.FeatureRowProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

/**
 * Converts {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into BigQuery schema. Serializes
 * it into json-like format {@link TableSchema}. Fetches existing schema to merge existing fields
 * with new ones.
 *
 * <p>As a side effect this Operation may create bq table (if it doesn't exist) to make
 * bootstrapping faster
 */
@SuppressWarnings("serial")
public class InsertFeatureSetToTable extends DoFn<FeatureRowProto.FeatureRow, String> {

  private JdbcTemplater jdbcTemplater;
  private JdbcConfig jdbcConfig;
  private String jobName;

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(InsertFeatureSetToTable.class);

  public InsertFeatureSetToTable(
      JdbcTemplater jdbcTemplater, JdbcConfig jdbcConfig, String jobName) {
    this.jdbcTemplater = jdbcTemplater;
    this.jdbcConfig = jdbcConfig;
    this.jobName = jobName;
  }

  @ProcessElement
  public void processElement(
      @Element FeatureRowProto.FeatureRow element,
      OutputReceiver<String> out,
      ProcessContext context) {
    String tableName = JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet());

    try {
      Class.forName(this.jdbcConfig.getClassName());
      Properties props = new Properties();
      props.put("user", this.jdbcConfig.getUsername());
      props.put("password", this.jdbcConfig.getPassword());
      props.put("db", this.jdbcConfig.getDatabase());
      props.put("schema", this.jdbcConfig.getSchema());
      props.put("tracing", "ALL");
      props.put("warehouse", this.jdbcConfig.getWarehouse());
      Connection conn = DriverManager.getConnection(this.jdbcConfig.getUrl(), props);

      DatabaseMetaData meta = conn.getMetaData();

      if (!meta.getTables(null, null, tableName.toUpperCase(), null).next()) {
        log.info("Table doesnot exists");
        FeatureSetProto.FeatureSetSpec spec =
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setName(element.getFeatureSet().split("/")[1])
                .setProject(element.getFeatureSet().split("/")[0])
                .build();

        Map<String, String> requiredColumns = this.jdbcTemplater.getRequiredColumns();
        String createSqlTableCreationQuery = this.jdbcTemplater.getTableCreationSql(spec);
        Statement stmt = conn.createStatement();
        stmt.execute(createSqlTableCreationQuery);
      }
      String insertQuery = this.jdbcTemplater.getFeatureRowInsertSql(element, this.jobName);
      Statement statement = conn.createStatement();
      statement.executeQuery(insertQuery);
      conn.close();

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s",
              this.jdbcConfig.getUrl(), this.jdbcConfig.getClassName()),
          e);
    }

    out.output(tableName);
  }
}
