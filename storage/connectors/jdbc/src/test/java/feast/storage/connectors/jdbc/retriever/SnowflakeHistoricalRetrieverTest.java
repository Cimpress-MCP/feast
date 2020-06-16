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
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SnowflakeHistoricalRetrieverTest {
  private JdbcHistoricalRetriever snowflakeFeatureRetriever;
  //  Snowflake account
  private String staging_location = System.getenv("staging_location");
  private Map<String, String> snowflakeConfig = new HashMap<>();
  private String SFUrl = "jdbc:snowflake://ry42518.us-east-2.aws.snowflakecomputing.com";
  private String SFClassName = "net.snowflake.client.jdbc.SnowflakeDriver";
  private String SFusername = "CHIZHANG";
  private String SFpw = "123456Pw";
  private String SFDatabase = "DEMO_DB";
  private String SFSchema = "PUBLIC";

  @Before
  public void setUp() {

    snowflakeConfig.put("database", SFDatabase);
    snowflakeConfig.put("schema", SFSchema);
    snowflakeConfig.put("class_name", SFClassName);
    snowflakeConfig.put("username", SFusername);
    snowflakeConfig.put("password", SFpw);
    snowflakeConfig.put("url", SFUrl);
    snowflakeConfig.put("staging_location", staging_location);

    snowflakeFeatureRetriever =
        (JdbcHistoricalRetriever) JdbcHistoricalRetriever.create(snowflakeConfig);
  }

  @Test
  public void shouldRetrieveFromSnowflake() {
    //      Set CSV format DATA_FORMAT_CSV = 2; where the first column of the csv file must be
    // entity_id
    //      file_uri is under
    // src/test/java/feast/storage/connectors/jdbc/retriever/snowflake_proj_entity_rows.csv
    String file_uris = System.getenv("snowflake_file_uri");
    ServingAPIProto.DatasetSource.FileSource fileSource =
        ServingAPIProto.DatasetSource.FileSource.newBuilder()
            .setDataFormatValue(2)
            .addFileUris(file_uris)
            .build();

    ServingAPIProto.DatasetSource datasetSource =
        ServingAPIProto.DatasetSource.newBuilder().setFileSource(fileSource).build();

    String retrievalId = "1234";
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                ServingAPIProto.FeatureReference.newBuilder()
                    .setName("feature_1")
                    .setProject("myproject2")
                    .setFeatureSet("feature_set")
                    .build())
            .build();
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureSetRequests.add(featureSetRequest);

    HistoricalRetrievalResult postgresHisRetrievalResult =
        snowflakeFeatureRetriever.getHistoricalFeatures(
            retrievalId, datasetSource, featureSetRequests);

    List<String> files = postgresHisRetrievalResult.getFileUris();
    File testFile = new File(files.get(0));
    // TODO: next step: check is a file
    //    Assert.assertTrue(!testFile.isDirectory());
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(files.get(0).indexOf(staging_location) != -1);
  }

  private FeatureSetProto.FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetProto.FeatureSetSpec.newBuilder()
        .setProject("myproject2")
        .setName("feature_set")
        .addEntities(FeatureSetProto.EntitySpec.newBuilder().setName("entity_id_primary"))
        .addFeatures(FeatureSetProto.FeatureSpec.newBuilder().setName("feature_1"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }
}
