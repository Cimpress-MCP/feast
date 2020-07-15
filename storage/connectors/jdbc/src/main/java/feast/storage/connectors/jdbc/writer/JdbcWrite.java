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

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FailedElement;
import feast.storage.api.writer.WriteResult;
//import feast.storage.connectors.bigquery.writer.BigQueryWrite.FeatureDynamicDestinations;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
//import feast.storage.connectors.jdbc.writer.JdbcFeatureSink.SubscribedFeatures;

import java.io.Serializable;
import java.sql.PreparedStatement;
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

//  public JdbcWrite(JdbcConfig config, JdbcTemplater jdbcTemplater,
//		  PCollection<KV<String, FeatureSet>> subscribedFeatureSets) {
//	  System.out.println("inside JDBCWrite constructor");
//	  this.config = config;
//	  this.jdbcTemplater = jdbcTemplater;
//	   this.subscribedFeatureSets = subscribedFeatureSets;
//  }


public JdbcWrite(JdbcConfig config, JdbcTemplater jdbcTemplater) {
	
	this.config = config;
	  this.jdbcTemplater = jdbcTemplater;

}


public StoreProto.Store.JdbcConfig getConfig() {
    return config;
  }

  @Override
  public WriteResult expand(PCollection<FeatureRowProto.FeatureRow> input) {
    String jobName = input.getPipeline().getOptions().getJobName();
    
//    FeatureDynamicDestinations dynamicDestinations = new FeatureDynamicDestinations();

    // Create a map of feature set references to incrementing partition numbers. This map will split
    // partition
    // The incoming feature rows an allow each of them to have a different INSERT statement based on
    // their feature set
    Map<String, Integer> featureSetToPartitionMap = new HashMap<>();

//     Check the featureset
//    PCollection<Map<String, String>> sqlInsertStatements = input.apply("SqlStatements",
//            ParDo.of(
//                    new DoFn<FeatureRowProto.FeatureRow , Map<String,String>>() {
//
//                  	@ProcessElement
//                  	public void process(
//                  		
//                  		 @Element FeatureRowProto.FeatureRow element,
//                  	     OutputReceiver <Map<String,String>> out,
//                  	     ProcessContext context) {
//                  		
//                  System.out.println("JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet())"+JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet()));
//                  		String featureRowInsertSql = jdbcTemplater.getFeatureRowInsertSql(element.getFeatureSet());
//                        sqlInsertStatements.put(JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet()), featureRowInsertSql);
//                        sqlInsertStatements.  
//                  			out.output(sqlInsertStatements);
//                  		}
//                  	
//                  	
//                    }
//                  		    ));
    
//    PCollectionList<FeatureRowProto.FeatureRow> partitionedInput =
//            applyPartitioningToPCollectionBasedOnFeatureSet(input, featureSetToPartitionMap);

//    for (String featureSetRef : subscribedFeatureSets.keySet()) {
//        // For this feature set reference find its partition number
//        int partitionNumber = featureSetToPartitionMap.get(featureSetRef);
//
//        // Find the PCollection that is associated with a partition number
//        PCollection<FeatureRowProto.FeatureRow> featureSetInput =
//            partitionedInput.get(partitionNumber);

        // Find the feature set spec associated with this partition
        FeatureSetProto.FeatureSetSpec currentFeatureSetSpec =
            subscribedFeatureSets.get(featureSetRef).getSpec();
        StoreProto.Store.JdbcConfig jdbcConfig = this.getConfig();
String getTableName = JdbcTemplater.getTableNameFromFeatureSet(element.getFeatureSet());
        

input.apply("SqlStatements",
      ParDo.of(
              new DoFn<FeatureRowProto.FeatureRow , Map<String,String>>() {

            	@ProcessElement
            	public void process(
            		
            		 @Element FeatureRowProto.FeatureRow element,
            	     OutputReceiver <Map<String,String>> out,
            	     ProcessContext context) {
            		
            		int batchSize = jdbcConfig.getBatchSize() > 0 ? jdbcConfig.getBatchSize() : 1;
            	
            	element.apply(
                String.format("WriteFeatureRowToJdbcIO-%s", jdbcTemplater.getFeatureRowInsertSql(input.getName()));
                JdbcIO.<FeatureRowProto.FeatureRow>write()
                    .withDataSourceConfiguration(create_dsconfig(jdbcConfig))
                    .withStatement(jdbcTemplater.getFeatureRowInsertSql(input.getName()))
                    .withBatchSize(batchSize)
                    .withPreparedStatementSetter(
                        new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
                          public void setParameters(
                              FeatureRowProto.FeatureRow element, PreparedStatement preparedStatement) {
                            jdbcTemplater.setSinkParameters(
                                element, preparedStatement, jobName);
                          }
                        }));
          }
        // Apply the WriteFeatureRow transformation to this feature set partitioned input
//        applyWriteFeatureRowToJdbcIo(
//            jobName,
//            featureSetRef,
//            featureSetInput,
//            currentFeatureSetSpec,
//            jdbcConfig,
//            jdbcTemplater);
//      }

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
}


  private static void applyWriteFeatureRowToJdbcIo(
      String jobName,
      String featureSetRef,
      PCollection<FeatureRowProto.FeatureRow> featureSetInput,
      String currentFeatureSetSpec,
      StoreProto.Store.JdbcConfig jdbcConfig,
      JdbcTemplater jdbcTemplater) {

    int batchSize = jdbcConfig.getBatchSize() > 0 ? jdbcConfig.getBatchSize() : 1;

    featureSetInput.apply(
        String.format("WriteFeatureRowToJdbcIO-%s", featureSetRef),
        JdbcIO.<FeatureRowProto.FeatureRow>write()
            .withDataSourceConfiguration(create_dsconfig(jdbcConfig))
            .withStatement(jdbcTemplater.getFeatureRowInsertSql(currentFeatureSetSpec))
            .withBatchSize(batchSize)
            .withPreparedStatementSetter(
                new JdbcIO.PreparedStatementSetter<FeatureRowProto.FeatureRow>() {
                  public void setParameters(
                      FeatureRowProto.FeatureRow element, PreparedStatement preparedStatement) {
                    jdbcTemplater.setSinkParameters(
                        element, preparedStatement, jobName);
                  }
                }));
  }

  private static JdbcIO.DataSourceConfiguration create_dsconfig(
      StoreProto.Store.JdbcConfig jdbcConfig) {
    String username = jdbcConfig.getUsername();
    String password = jdbcConfig.getPassword();
    String className = jdbcConfig.getClassName();
    String url = jdbcConfig.getUrl();

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
  


