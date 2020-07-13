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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

import com.google.api.services.bigquery.model.TableSchema;

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.storage.connectors.jdbc.common.JdbcTemplater;

/**
 * Converts {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into BigQuery schema. Serializes
 * it into json-like format {@link TableSchema}. Fetches existing schema to merge existing fields
 * with new ones.
 *
 * <p>As a side effect this Operation may create bq table (if it doesn't exist) to make
 * bootstrapping faster
 */
@SuppressWarnings("serial")
public class FeatureSetSpecToTableSchemaJDBC
    extends DoFn<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>, FeatureSetReference> {
	
  private JdbcTemplater jdbcTemplater;
  private JdbcConfig jdbcConfig;

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureSetSpecToTableSchemaJDBC.class);

  public FeatureSetSpecToTableSchemaJDBC(JdbcTemplater jdbcTemplater, JdbcConfig jdbcConfig) {
    this.jdbcTemplater = jdbcTemplater;
    this.jdbcConfig = jdbcConfig;
  }

  @ProcessElement
  public void processElement(
      @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
      OutputReceiver<FeatureSetReference> out,
      ProcessContext context) {

    FeatureSetProto.FeatureSetSpec featureSetSpec = element.getValue();

    System.out.println("inside FeatureSetSpecToTableSchema ////" + featureSetSpec.getName());
    String featureSetRef = JdbcTemplater.getTableName(featureSetSpec);
    System.out.println("featureSetRef ////" + featureSetRef);
//    Map<String, String> requiredColumns = this.jdbcTemplater.getRequiredColumns(featureSetSpec);
    Map<String, String> requiredColumns = this.jdbcTemplater.getRequiredColumns();
    Properties props = new Properties();
    props.put("user", this.jdbcConfig.getUsername());
    props.put("password", this.jdbcConfig.getPassword());
    props.put("db", this.jdbcConfig.getDatabase());
    props.put("schema", this.jdbcConfig.getSchema());

    try {
      Class.forName(this.jdbcConfig.getClassName());
      Connection conn = DriverManager.getConnection(this.jdbcConfig.getUrl(), props);
      String createSqlTableCreationQuery = this.jdbcTemplater.getTableCreationSql(featureSetSpec);
      Statement stmt = conn.createStatement();
      stmt.execute(createSqlTableCreationQuery);

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s",
              this.jdbcConfig.getUrl(), this.jdbcConfig.getClassName()),
          e);
    }

    
    out.output(element.getKey());
  }

  public static String getFeatureSetRef(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }

  //  private static String createTablequery(
  //		     JdbcTemplater jdbcTemplater, FeatureSetProto.FeatureSetSpec featureSetSpec) {
  //		    String createSqlTableCreationQuery = jdbcTemplater.getTableCreationSql(featureSetSpec);
  //		    return createSqlTableCreationQuery;
  //		    }
  //
  //  private void createTable(
  //	      Connection conn, JdbcTemplater jdbcTemplater, FeatureSetProto.FeatureSetSpec
  // featureSetSpec) {
  //	    String featureSetName = getFeatureSetRef(featureSetSpec);
  //	    String createSqlTableCreationQuery = jdbcTemplater.getTableCreationSql(featureSetSpec);
  //	    try {
  //	      Statement stmt = conn.createStatement();
  //	      stmt.execute(createSqlTableCreationQuery);
  //	    } catch (SQLException e) {
  //	      throw new RuntimeException(
  //	          String.format("Could not create table for feature set %s", featureSetName), e);
  //	    }
  //	  }

}