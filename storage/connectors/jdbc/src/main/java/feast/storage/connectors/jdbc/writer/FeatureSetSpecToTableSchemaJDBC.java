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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import feast.storage.connectors.jdbc.connection.JdbcConnectionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

/**
 * Converts {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into BigQuery schema. Serializes
 * it into json-like format {@link TableSchema}. Fetches existing schema to merge existing fields
 * with new ones.
 *
 * <p>As a side effect this Operation may create bq table (if it doesn't exist) to make
 * bootstrapping faster
 */
public class FeatureSetSpecToTableSchemaJDBC
    extends DoFn<
        KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>,
        FeatureSetReference> {
	private JdbcConnectionProvider connectionProvider;
	private JdbcTemplater jdbcTemplater;
  
  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(FeatureSetSpecToTableSchemaJDBC.class);

  // Reserved columns
  public static final String EVENT_TIMESTAMP_COLUMN = "event_timestamp";
  public static final String CREATED_TIMESTAMP_COLUMN = "created_timestamp";
  public static final String INGESTION_ID_COLUMN = "ingestion_id";
  public static final String JOB_ID_COLUMN = "job_id";

  // Column description for reserved fields
  public static final String JDBC_EVENT_TIMESTAMP_FIELD_DESCRIPTION =
      "Event time for the FeatureRow";
  public static final String JDBC_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String JDBC_INGESTION_ID_FIELD_DESCRIPTION =
      "Unique id identifying groups of rows that have been ingested together";
  public static final String JDBC_JOB_ID_FIELD_DESCRIPTION =
      "Feast import job ID for the FeatureRow";

//  public FeatureSetSpecToTableSchemaJDBC( JdbcConfig dsconfig) {
//	  this.config = dsconfig;
//	  
//  }
  public FeatureSetSpecToTableSchemaJDBC(JdbcConnectionProvider connectionProvider, JdbcTemplater jdbcTemplater) {
	  this.connectionProvider = connectionProvider;
	  this.jdbcTemplater = jdbcTemplater;
//	  
  }
  
  public FeatureSetSpecToTableSchemaJDBC(JdbcTemplater jdbcTemplater) {
	  this.jdbcTemplater = jdbcTemplater;
//	  
  }
//
//  @Setup
//  public void setup() {
//    this.bqService = bqProvider.get();
//  }

  @ProcessElement
  public void processElement(
      @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
      OutputReceiver <FeatureSetReference> output,
      ProcessContext context) {
    String specKey = element.getKey().getReference();
    FeatureSetProto.FeatureSetSpec featureSetSpec = element.getValue();
    
    System.out.println("inside FeatureSetSpecToTableSchema ////"+ featureSetSpec.getName());
    String featureSetRef = getFeatureSetRef(featureSetSpec);
    System.out.println("featureSetRef ////"+ featureSetRef);
    String query = createTable(this.jdbcTemplater, featureSetSpec);
    System.out.println("query ////"+ query);
    
   

//    element.apply(JdbcIO.write()	
//			.withDataSourceConfiguration(dsconfig)
//			.withStatement(query));
////		          .withQuery(query));
//			.withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())).withRowMapper(
//					return KV.of(1, "Success")));

    
//    if (tableExists(this.conn, featureSetSpec, this.config)) {
//      updateTable(conn, this.getJdbcTemplater(), featureSetSpec);
//    } else {
//      createTable(conn, this.getJdbcTemplater(), featureSetSpec);
//    }
//
//    Table existingTable = getExistingTable(specKey);
//    Schema schema = createSchemaFromSpec(element.getValue(), specKey, existingTable);
//
//    if (existingTable == null) {
//      createTable(specKey, schema);
//    }
    	output.output(element.getKey());
//    output.output(KV.of(element.getKey(), featureSetSpec));
  }

  public static String getFeatureSetRef(FeatureSetProto.FeatureSetSpec featureSetSpec) {
	    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
	  }
  
  private static String createTable(
		     JdbcTemplater jdbcTemplater, FeatureSetProto.FeatureSetSpec featureSetSpec) {
		    String createSqlTableCreationQuery = jdbcTemplater.getTableCreationSql(featureSetSpec);
		    return createSqlTableCreationQuery;
		    }
  
  

}

