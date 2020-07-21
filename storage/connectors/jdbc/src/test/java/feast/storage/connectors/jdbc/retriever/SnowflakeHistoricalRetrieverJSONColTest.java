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
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.connectors.jdbc.connection.SnowflakeConnectionProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SnowflakeHistoricalRetrieverJSONColTest {
  private JdbcHistoricalRetriever snowflakeFeatureRetriever;
  //  Snowflake account
  //  private String staging_location = System.getenv("STAGING_LOCATION");
  private String staging_location = "s3://feast-snowflake-staging/test/";

  private Map<String, String> snowflakeConfig = new HashMap<>();
  private String SFUrl = "jdbc:snowflake://ry42518.us-east-2.aws.snowflakecomputing.com";
  private String SFClassName = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String SFusername = System.getenv("SNOWFLAKE_USERNAME_RETRI");
  private String SFpw = System.getenv("SNOWFLAKE_PASSWORD_RETRI");
  private String SFDatabase = "DEMO_DB";
  private String SFSchema = "PUBLIC";
  private String SFRole = "ACCOUNTADMIN";

  @Before
  public void setUp() {

    snowflakeConfig.put("database", SFDatabase);
    snowflakeConfig.put("schema", SFSchema);
    snowflakeConfig.put("class_name", SFClassName);
    snowflakeConfig.put("username", SFusername);
    snowflakeConfig.put("password", SFpw);
    snowflakeConfig.put("url", SFUrl);
    snowflakeConfig.put("staging_location", staging_location);
    snowflakeConfig.put("role", SFRole);
    SnowflakeConnectionProvider snowflakeConnectionProvider =
        new SnowflakeConnectionProvider(snowflakeConfig);
    SnowflakeQueryTemplater snowflakeQueryTemplater =
        new SnowflakeQueryTemplater(snowflakeConnectionProvider);
    snowflakeFeatureRetriever =
        (JdbcHistoricalRetriever)
            JdbcHistoricalRetriever.create(snowflakeConfig, snowflakeQueryTemplater);
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest2Dates() {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_2dates.csv
    String file_uris = System.getenv("ENTITIES_URI_2DATES");
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();
    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequests();
    HistoricalRetrievalResult postgresHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = postgresHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,400 2,100 3,300 */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest1Date() {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_1date.csv
    String file_uris = System.getenv("ENTITIES_URI_1DATE");
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();

    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequests();
    HistoricalRetrievalResult postgresHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = postgresHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,410 2,220 3,300 */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  @Test
  public void shouldRetrieveFromSnowflakeTest1DateWithNull() {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_1date_null.csv
    String file_uris = System.getenv("ENTITIES_URI_1DATE_NULL");
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();

    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequests();
    HistoricalRetrievalResult postgresHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = postgresHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,410 2,100 3,null */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  @Test
  public void shouldRetrieveFromSnowflakeTestSameIds() {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/entities_2dates.csv
    String file_uris = System.getenv("ENTITIES_URI_SAME_IDS");
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();
    String retrievalId = "1234";
    List<FeatureSetRequest> featureSetRequests = this.createFeatureSetRequests();
    HistoricalRetrievalResult postgresHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests, false);

    List<String> files = postgresHisRetrievalResult.getFileUris();
    /** Should return ENTITY_ID_PRIMARY, FEATURE_SET__FEATURE_1 1,null 2,100 3,300 1,410 */
    Assert.assertTrue(files.get(0).contains(staging_location));
  }

  private List<FeatureSetRequest> createFeatureSetRequests() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_1")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_2")
                    .setProject("myproject4")
                    .setFeatureSet("feature_set")
                    .build())
            .build();
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureSetRequests.add(featureSetRequest);
    return featureSetRequests;
  }

  private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetProto.FeatureSetSpec.newBuilder()
        .setProject("myproject4")
        .setName("feature_set")
        .addEntities(FeatureSetProto.EntitySpec.newBuilder().setName("entity_id_primary"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }
}
