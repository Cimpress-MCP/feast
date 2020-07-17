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
package feast.storage.connectors.jdbc.common;

import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto;
import java.io.Serializable;
import java.util.Map;

public interface JdbcTemplater extends Serializable {
  static String getTableName(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    return String.format("%s_%s", featureSetSpec.getProject(), featureSetSpec.getName())
        .replaceAll("-", "_");
  }

  static String getTableNameFromFeatureSet(String featureSet) {
    String tablename = featureSet.replaceAll("/", "_");
    return tablename.replaceAll("-", "_");
  }

  String getTableCreationSql(FeatureSetProto.FeatureSetSpec featureSetSpec);

  String getFeatureRowInsertSql(FeatureRowProto.FeatureRow element, String jobName);

  Map<String, String> getRequiredColumns();
}
