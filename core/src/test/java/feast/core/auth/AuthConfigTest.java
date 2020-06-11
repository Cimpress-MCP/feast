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
package feast.core.auth;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import feast.core.config.FeastProperties;
import feast.core.config.SecurityConfig;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { AuthTestConfig.class, SecurityConfig.class})
@TestPropertySource(locations = { "classpath:application.properties", "classpath:application.yml"} )
class AuthConfigTest {
  
  @Inject List<AuthenticationProvider> authProviders;
  @Inject AuthenticationManager authManager;
  @Inject AuthTestConfig config;

  
  @Test
  void canConfigureAuth() {
    assertNotNull(authManager);
  }
  
  @Test
  void canParseEl() {
    
    StandardEvaluationContext ctx = new StandardEvaluationContext(config);

    
    ExpressionParser parser = new SpelExpressionParser();
    Expression exp = parser.parseExpression("feast.security?.authentication?.provider == 'jwt'");
    Boolean result = exp.getValue(ctx, Boolean.class);
    assertTrue(result);
  }

}
