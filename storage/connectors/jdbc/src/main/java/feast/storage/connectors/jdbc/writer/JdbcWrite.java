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

import feast.proto.core.FeatureSetReferenceProto;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.jdbc.common.JdbcTemplater;


import java.io.Serializable;
import java.sql.PreparedStatement;

import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

/**
 * A {@link PTransform} that writes {@link FeatureRowProto FeatureRows} to the specified BigQuery
 * dataset, and returns a {@link WriteResult} containing the unsuccessful writes. Since Bigquery
 * does not output successful writes, we cannot emit those, and so no success metrics will be
 * captured if this sink is used.
 */
public class JdbcWrite extends PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcWrite.class);	
  private final JdbcTemplater jdbcTemplater;
  private final StoreProto.Store.JdbcConfig config;
  private PCollectionView<Map<String, Iterable<String>>> subscribedFeatureSets;
  private PCollection<KV<String, String>> subscribedTables;


public JdbcWrite(JdbcConfig config, JdbcTemplater jdbcTemplater) {
	
	this.config = config;
	  this.jdbcTemplater = jdbcTemplater;

}

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();

  
//	input.apply("WriteToTable",
//	      ParDo.of(
//	              new JdbcWriterHelper(this.getJdbcTemplater(), this.getConfig(), jobName)));
	 int batchSize = this.config.getBatchSize() > 0 ? config.getBatchSize() : 1;


            input.apply(JdbcIO.<FeatureRowProto.FeatureRow>write()
        	        .withDataSourceConfiguration(create_dsconfig(this.config))
//        	        .withStatement(this.jdbcTemplater.getFeatureRowInsertSql(JdbcTemplater.getTableNameFromFeatureSet())
        	        .withStatement("INSERT into snowflake_proj_feature_set_4 (event_timestamp,created_timestamp,feature,ingestion_id,job_id) select (?,?, parse_json(?),?,?)")
        	        .withBatchSize(batchSize)
        	        .withPreparedStatementSetter(
        	            new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
        	            	
					@Override
					public void setParameters(FeatureRow element, PreparedStatement preparedStatement)
							throws Exception {
						jdbcTemplater.setSinkParameters(element, preparedStatement, jobName);
						
					}}));
            

        PCollection<FeatureRowProto.FeatureRow> successfulInserts =
            input.apply(

                "dummy",
                ParDo.of(
                    new DoFn<FeatureRowProto.FeatureRow, FeatureRowProto.FeatureRow>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {}
                    }));

        PCollection<FailedElement> failedElements =
            input.apply(
                "dummyFailed",
                ParDo.of(
                    new DoFn<FeatureRowProto.FeatureRow, FailedElement>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {}
                    }));

        return WriteResult.in(input.getPipeline(), successfulInserts, failedElements);

  }




  
  
  public static JdbcIO.DataSourceConfiguration create_dsconfig(
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