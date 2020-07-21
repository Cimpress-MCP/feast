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
package feast.serving.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.StoreProto;
import feast.serving.service.*;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.bigquery.retriever.BigQueryHistoricalRetriever;
import feast.storage.connectors.jdbc.connection.PostgresConnectionProvider;
import feast.storage.connectors.jdbc.connection.SnowflakeConnectionProvider;
import feast.storage.connectors.jdbc.retriever.JdbcHistoricalRetriever;
import feast.storage.connectors.jdbc.retriever.PostgresQueryTemplater;
import feast.storage.connectors.jdbc.retriever.SnowflakeQueryTemplater;
import feast.storage.connectors.redis.retriever.RedisClusterOnlineRetriever;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import io.opentracing.Tracer;
import java.util.Map;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServingServiceConfig {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfig.class);

  @Bean
  public ServingService servingService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      JobService jobService,
      Tracer tracer)
      throws InvalidProtocolBufferException, JsonProcessingException {
    ServingService servingService = null;
    FeastProperties.Store store = feastProperties.getActiveStore();
    StoreProto.Store.StoreType storeType = store.toProto().getType();
    Map<String, String> config = store.getConfig();

    switch (storeType) {
      case REDIS_CLUSTER:
        OnlineRetriever redisClusterRetriever = RedisClusterOnlineRetriever.create(config);
        servingService = new OnlineServingService(redisClusterRetriever, specService, tracer);
        break;
      case REDIS:
        OnlineRetriever redisRetriever = RedisOnlineRetriever.create(config);
        servingService = new OnlineServingService(redisRetriever, specService, tracer);
        break;
      case BIGQUERY:
        validateJobServicePresence(jobService);
        HistoricalRetriever bqRetriever = BigQueryHistoricalRetriever.create(config);
        servingService = new HistoricalServingService(bqRetriever, specService, jobService);
        break;
      case JDBC:
        validateJobServicePresence(jobService);
        HistoricalRetriever jdbcHistoricalRetriever =
            this.createJdbcHistoricalRetriever(feastProperties);
        servingService =
            new HistoricalServingService(jdbcHistoricalRetriever, specService, jobService);
        break;
      case CASSANDRA:
      case UNRECOGNIZED:
      case INVALID:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported store type '%s' for store name '%s'",
                store.getType(), store.getName()));
    }

    return servingService;
  }

  @Bean
  public HistoricalRetriever createJdbcHistoricalRetriever(FeastProperties feastProperties) {
    FeastProperties.Store store = feastProperties.getActiveStore();
    Map<String, String> config = store.getConfig();
    String className = config.get("class_name");
    switch (className) {
      case "net.snowflake.client.jdbc.SnowflakeDriver":
        SnowflakeConnectionProvider snowflakeConnectionProvider =
            new SnowflakeConnectionProvider(config);
        SnowflakeQueryTemplater snowflakeQueryTemplater =
            new SnowflakeQueryTemplater(config, snowflakeConnectionProvider);
        return JdbcHistoricalRetriever.create(config, snowflakeQueryTemplater);
      case "org.postgresql.Driver":
        PostgresConnectionProvider postgresConnectionProvider =
            new PostgresConnectionProvider(config);
        PostgresQueryTemplater postgresQueryTemplater =
            new PostgresQueryTemplater(config, postgresConnectionProvider);
        return JdbcHistoricalRetriever.create(config, postgresQueryTemplater);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported JDBC store className '%s' for store name '%s'",
                className, store.getName()));
    }
  }

  private void validateJobServicePresence(JobService jobService) {
    if (jobService.getClass() == NoopJobService.class) {
      throw new IllegalArgumentException(
          "Job service has not been instantiated. The Job service is required for all historical stores.");
    }
  }
}
