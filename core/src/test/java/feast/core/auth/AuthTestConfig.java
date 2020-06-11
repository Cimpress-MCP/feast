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
package feast.core.auth;

import java.util.Properties;
import javax.inject.Inject;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import feast.core.config.FeastProperties;

/** Configuration of JPA related services and beans for the core application. */
@TestConfiguration
@EnableConfigurationProperties(FeastProperties.class)
public class AuthTestConfig {
  
  public @Inject FeastProperties feast;
  
  @Bean 
  public BuildProperties buildProperties() {
    Properties props = new Properties();
    props.put("version", "test");
    return new BuildProperties(props);
  }
  
}
