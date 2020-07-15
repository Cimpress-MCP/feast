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
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import feast.storage.connectors.jdbc.postgres.PostgresqlTemplater;
import feast.storage.connectors.jdbc.snowflake.SnowflakeTemplater;
import feast.storage.connectors.jdbc.sqlite.SqliteTemplater;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;

public class JdbcFeatureSink implements FeatureSink {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcFeatureSink.class);

  private PCollectionView<Map<String, Iterable<FeatureSet>>> subscribedFeatureSets;
  
  private PCollectionView<Map<String, Iterable<String>>> subscribedTable;
  private final StoreProto.Store.JdbcConfig config;

  public JdbcTemplater getJdbcTemplater() {
    return jdbcTemplater;
  }

  private JdbcTemplater jdbcTemplater;



  public JdbcFeatureSink(JdbcConfig config) {
    this.config = config;
    this.jdbcTemplater = getJdbcTemplaterForClass(config.getClassName());
  }

  private JdbcTemplater getJdbcTemplaterForClass(String className) {
    switch (className) {
      case "org.sqlite.JDBC":
        return new SqliteTemplater();
      case "org.postgresql.Driver":
        return new PostgresqlTemplater();
      case "net.snowflake.client.jdbc.SnowflakeDriver":
        return new SnowflakeTemplater();
      default:
        throw new RuntimeException(
            "JDBC class name was not specified, was incorrect, or had no implementation for templating.");
    }
  }

  public static FeatureSink fromConfig(JdbcConfig config) {
    return new JdbcFeatureSink(config);
  }

  public JdbcConfig getConfig() {
    return config;
  }

  /**
   * @param KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>
   * @return FeatureSetReference
   */
  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs) {

	  
	  this.subscribedTable = featureSetSpecs.apply(
	            "GetSubscribedFeatureSets",
	            ParDo.of(
	                new SubscribedTable()))
			  
			  .apply("View", View.asMultimap());
	  
	  System.out.println(" this.subscribedTable----"+ this.subscribedTable);
	  
    PCollection<FeatureSetReference> schemas =
        featureSetSpecs.apply(
            "CreateTableSchema",
            ParDo.of(
                new FeatureSetSpecToTableSchemaJDBC(this.getJdbcTemplater(), this.getConfig())));
 
   	  return schemas;
  }

  public static String getFeatureSetRef(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
  }

  public PCollectionView<Map<String, Iterable<FeatureSet>>> getSubscribedFeatureSets() {
    return this.subscribedFeatureSets;
  }
  
  public PCollectionView<Map<String, Iterable<String>>> getSubscribedTables() {
	    return this.subscribedTable;
	  }

//  @Override
//  public JdbcWrite writer() {
//    return new JdbcWrite(
//        this.getConfig(), this.getJdbcTemplater(), this.getSubscribedFeatureSets());
//  }
  
  @Override
public JdbcWrite writer() {
  return new JdbcWrite(
      this.getConfig(), this.getJdbcTemplater(), this.getSubscribedTables());
}
  
//  public static class SubscribedFeatures
//  extends DoFn<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>, KV<String, FeatureSetProto.FeatureSet>> {
//
//	@ProcessElement
//	public void process(
//		
//		 @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
//	     OutputReceiver <KV<String, FeatureSetProto.FeatureSet>> out,
//	     ProcessContext context) {
//		
//		System.out.println("SubscribedFeatures--");
//			FeatureSetProto.FeatureSetSpec featureSetSpec = element.getValue();
//		    String featureSetKey = JdbcTemplater.getTableName(featureSetSpec);
//		    System.out.println("featureSetKey----"+featureSetKey);
//		    FeatureSetProto.FeatureSet featureSet = FeatureSetProto.FeatureSet.newBuilder().setSpec(element.getValue()).build();
////		   
//		    System.out.println("featureSet----"+featureSet);
//		    	
//	  out.output(KV.of(featureSetKey, featureSet));
//	}
//  }


public static class SubscribedTable
extends DoFn<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>, KV<String, String>> {

	@ProcessElement
	public void process(
		
		 @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
	     OutputReceiver <KV<String, String>> out,
	     ProcessContext context) {
		
		System.out.println("SubscribedFeatures--");
			FeatureSetProto.FeatureSetSpec featureSetSpec = element.getValue();
		    String featureSetKey = JdbcTemplater.getTableName(featureSetSpec);
		    System.out.println("featureSetKey----"+featureSetKey);
		    FeatureSetProto.FeatureSet featureSet = FeatureSetProto.FeatureSet.newBuilder().setSpec(element.getValue()).build();
//		   
		    System.out.println("featureSet----"+featureSet);
		    	
	  out.output(KV.of(featureSetKey, featureSet.getSpec().getName()));
	}
}}




