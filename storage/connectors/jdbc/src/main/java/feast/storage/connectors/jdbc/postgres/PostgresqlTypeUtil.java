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
package feast.storage.connectors.jdbc.postgres;

import feast.proto.types.ValueProto;
import java.util.HashMap;
import java.util.Map;

public class PostgresqlTypeUtil {

  private static final Map<ValueProto.ValueType.Enum, String> VALUE_TYPE_TO_SQL_TYPE =
      new HashMap<>();

  static {
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.BYTES, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.STRING, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.INT32, "INTEGER");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.INT64, "BIGINT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.DOUBLE, "DOUBLE PRECISION");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.FLOAT, "FLOAT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.BOOL, "BOOLEAN");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.BYTES_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.STRING_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.INT32_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.INT64_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.DOUBLE_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.FLOAT_LIST, "TEXT");
    VALUE_TYPE_TO_SQL_TYPE.put(ValueProto.ValueType.Enum.BOOL_LIST, "TEXT");
  }

  public static String toSqlType(ValueProto.ValueType.Enum valueType) {
    return VALUE_TYPE_TO_SQL_TYPE.get(valueType);
  }
}
