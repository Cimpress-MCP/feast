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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.google.api.services.bigquery.model.TableSchema;

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import feast.storage.connectors.jdbc.snowflake.SnowflakeTemplater;

/**
 * Converts {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into BigQuery schema. Serializes
 * it into json-like format {@link TableSchema}. Fetches existing schema to merge existing fields
 * with new ones.
 *
 * <p>As a side effect this Operation may create bq table (if it doesn't exist) to make
 * bootstrapping faster
 */
@SuppressWarnings("serial")
public class JdbcWriterHelper
    extends DoFn<FeatureRowProto.FeatureRow, String> {
	
  private JdbcTemplater jdbcTemplater;
  private JdbcConfig jdbcConfig;
  private SnowflakeTemplater sfTemplater;
private String jobName;

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(JdbcWriterHelper.class);

  public JdbcWriterHelper(JdbcTemplater jdbcTemplater, JdbcConfig jdbcConfig , String jobName) {
    this.jdbcTemplater = jdbcTemplater;
    this.jdbcConfig = jdbcConfig;
    this.jobName = jobName;
  }

  @ProcessElement
  public void processElement(
      @Element FeatureRowProto.FeatureRow element,
      OutputReceiver <String> out,
      ProcessContext context) {

    int batchSize = this.jdbcConfig.getBatchSize() > 0 ? jdbcConfig.getBatchSize() : 1;
    String tableName = JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet());
    String jobName = this.jobName;
    JdbcTemplater jdbcTemplater = this.jdbcTemplater;
    System.out.println("JdbcWriterHelper --jobName--"+ jobName);
    System.out.println("JdbcWriterHelper --jdbcTemplater--"+ jdbcTemplater.getClass());
    System.out.println("JdbcWriterHelper --element--"+ element.getFeatureSet()+ "--");
    
    System.out.println(String.format("WriteFeatureRowToJdbcIO-%s", jdbcTemplater.getFeatureRowInsertSql(tableName)));
    	    JdbcIO.<FeatureRowProto.FeatureRow>write()
    	        .withDataSourceConfiguration(create_dsconfig(this.jdbcConfig))
//    	        .withStatement(this.jdbcTemplater.getFeatureRowInsertSql(tableName))
    	        .withStatement("INSERT(feature,ingestion_id,job_id) Values( parse_json(?),?,?)")
    	        .withBatchSize(batchSize)
    	        .withPreparedStatementSetter(
    	            new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
    	            	
    	            	Instant eventTsInstant =
    	            	          Instant.ofEpochSecond(element.getEventTimestamp().getSeconds())
    	            	              .plusNanos(element.getEventTimestamp().getNanos());
   	              public void setParameters(
   	            		  
  	                  FeatureRowProto.FeatureRow element, PreparedStatement preparedStatement) {
   	            	
   	               
   	           
   	        		try {
   	        			preparedStatement.setString(1, "{ 'entity_id_primary': 4}" );
   	     	           preparedStatement.setString(2, "somevalue");
						preparedStatement.setString(3, "jdbcsnowflakefeaturesinktest0shouldwritetosnowflake");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
   	        		
   	            	  
//    	            	  jdbcTemplater.setSinkParameters(element,preparedStatement, jobName);
////    	            	  setSinkParameters(element,preparedStatement, jobName);
    	              }});
    	        	            
//    
    out.output(element.getFeatureSet());
  }
  
  

  public static String getFeatureSetRef(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }

  private static JdbcIO.DataSourceConfiguration create_dsconfig(
	      StoreProto.Store.JdbcConfig jdbcConfig) {
	    String username = jdbcConfig.getUsername();
	    String password = jdbcConfig.getPassword();
	    String className = jdbcConfig.getClassName();
	    String url = jdbcConfig.getUrl();

	    System.out.println(String.format("dsconfig Warehouse() %s Database %s Schema %s:::",
	    		jdbcConfig.getWarehouse() ,
	    		jdbcConfig.getDatabase(),
	    		jdbcConfig.getSchema()));
	    if (className == "net.snowflake.client.jdbc.SnowflakeDriver") {
	      String database = jdbcConfig.getDatabase();
	      String schema = jdbcConfig.getSchema();
	      String warehouse = jdbcConfig.getWarehouse();
	      return JdbcIO.DataSourceConfiguration.create(className, url)
	          .withUsername(!username.isEmpty() ? username : null)
	          .withPassword(!password.isEmpty() ? password : null)
	          .withConnectionProperties(
	              String.format("warehouse=%s;db=%s;schema=%s", warehouse, database, schema));

	    } else {
	      return JdbcIO.DataSourceConfiguration.create(className, url)
	          .withUsername(!username.isEmpty() ? username : null)
	          .withPassword(!password.isEmpty() ? password : null);
	    }
	    
  }
 
  
}
