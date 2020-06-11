/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import static feast.storage.common.testing.TestUtil.field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.storage.api.writer.FeatureSink;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JdbcFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private FeatureSink sqliteFeatureSink;
  // TODO: Clean up url
  //  private String url = "jdbc:sqlite:/home/willem/Dump/sqldb/test.db";
  //  private String className = "org.sqlite.JDBC";

  private String url = "jdbc:postgresql://localhost:5432/postgres";
  private String className = "org.postgresql.Driver";
  private String userName = "postgres";

  private Connection conn;

  @Before
  public void setUp() {

    FeatureSetProto.FeatureSetSpec spec1 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("fs")
            .setProject("myproject2")
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity")
                    .setValueType(Enum.INT64)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature")
                    .setValueType(Enum.STRING)
                    .build())
            .build();

    FeatureSetProto.FeatureSetSpec spec2 =
        FeatureSetProto.FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject2")
            .addEntities(
                FeatureSetProto.EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_1")
                    .setValueType(Enum.STRING_LIST)
                    .build())
            .addFeatures(
                FeatureSetProto.FeatureSpec.newBuilder()
                    .setName("feature_2")
                    .setValueType(Enum.INT64)
                    .build())
            .build();

    Map<String, FeatureSetProto.FeatureSetSpec> specMap =
        ImmutableMap.of("myproject2/fs", spec1, "myproject2/feature_set", spec2);

    sqliteFeatureSink =
        JdbcFeatureSink.fromConfig(
            StoreProto.Store.JdbcConfig.newBuilder()
                .setUrl(this.url)
                .setClassName(this.className)
                .setUsername(this.userName)
                .setBatchSize(1) // This must be set to 1 for DirectRunner
                .build());

    sqliteFeatureSink.prepareWrite(FeatureSetProto.FeatureSet.newBuilder().setSpec(spec1).build());
    sqliteFeatureSink.prepareWrite(FeatureSetProto.FeatureSet.newBuilder().setSpec(spec2).build());

    this.connect();
  }

  private void connect() {
    if (conn != null) {
      return;
    }
    try {
      Class.forName(this.className);
      conn = DriverManager.getConnection(this.url, userName, null);
    } catch (ClassNotFoundException | SQLException e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    }
  }

  @Test
  public void shouldWriteToSqlite() {

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject2/fs")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("myproject2/fs")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("myproject2/fs")
                .addFields(field("entity", 3, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("myproject2/feature_set")
                .addFields(field("entity_id_primary", 4, Enum.INT32))
                .addFields(field("entity_id_secondary", "asjdh", Enum.STRING))
                .addFields(
                    FieldProto.Field.newBuilder()
                        .setName("feature_1")
                        .setValue(
                            ValueProto.Value.newBuilder()
                                .setStringListVal(
                                    ValueProto.StringList.newBuilder()
                                        .addVal("abc")
                                        .addVal("def")
                                        .build())
                                .build())
                        .build())
                .addFields(field("feature_2", 4, Enum.INT64))
                .build());

    p.apply(Create.of(featureRows)).apply(sqliteFeatureSink.writer());
    p.run();
    // TODO: Remove this assert, add SQL query
    assert (true);
  }
}
