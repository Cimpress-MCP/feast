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
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.JdbcConfig;
import feast.storage.api.writer.FeatureSink;
import feast.storage.connectors.jdbc.common.JdbcTemplater;
import feast.storage.connectors.jdbc.snowflake.SnowflakeTemplater;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

public class JdbcFeatureSink implements FeatureSink {
  /** */
  private static final long serialVersionUID = 1L;

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JdbcFeatureSink.class);

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

  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs) {

    PCollection<FeatureSetReference> schemas =
        featureSetSpecs.apply(
            "createSchema",
            ParDo.of(
                new DoFn<
                    KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>,
                    FeatureSetReference>() {

                  private static final long serialVersionUID = 1L;

                  @ProcessElement
                  public void processElement(
                      @Element KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec> element,
                      OutputReceiver<FeatureSetReference> out,
                      ProcessContext context) {
                    out.output(element.getKey());
                  }
                }));

    Map<String, String> requiredColumns = this.jdbcTemplater.getRequiredColumns();
    Properties props = new Properties();
    props.put("user", this.config.getUsername());
    props.put("password", this.config.getPassword());
    props.put("db", this.config.getDatabase());
    props.put("schema", this.config.getSchema());

    try {
      Class.forName(this.config.getClassName());
      Connection conn = DriverManager.getConnection(this.config.getUrl(), props);
      String createSqlTableCreationQuery = this.jdbcTemplater.getTableCreationSql(this.config);
      Statement stmt = conn.createStatement();
      stmt.execute(createSqlTableCreationQuery);

    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(
          String.format(
              "Could not connect to database with url %s and classname %s",
              this.config.getUrl(), this.config.getClassName()),
          e);
    }

    return schemas;
  }

  @Override
  public JdbcWrite writer() {
    return new JdbcWrite(this.getConfig(), this.getJdbcTemplater());
  }
}
