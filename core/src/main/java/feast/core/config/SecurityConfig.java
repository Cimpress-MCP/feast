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
package feast.core.config;

import feast.core.auth.authorization.AuthorizationProvider;
import feast.core.auth.authorization.Keto.KetoAuthorizationProvider;
import feast.core.config.FeastProperties.SecurityProperties;
import feast.proto.core.CoreServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import net.devh.boot.grpc.server.security.authentication.BearerAuthenticationReader;
import net.devh.boot.grpc.server.security.authentication.GrpcAuthenticationReader;
import net.devh.boot.grpc.server.security.check.AccessPredicate;
import net.devh.boot.grpc.server.security.check.AccessPredicateVoter;
import net.devh.boot.grpc.server.security.check.GrpcSecurityMetadataSource;
import net.devh.boot.grpc.server.security.check.ManualGrpcSecurityMetadataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.vote.UnanimousBased;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.BearerTokenAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;

@Configuration
public class SecurityConfig {

  private final SecurityProperties securityProperties;

  @Inject private List<AuthenticationProvider> authenticationProviders;

  public SecurityConfig(FeastProperties feastProperties) {
    this.securityProperties = feastProperties.getSecurity();
  }

  /**
   * Initializes an AuthenticationManager if authentication has been enabled.
   *
   * @return AuthenticationManager
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  public AuthenticationManager authenticationManager() throws Exception {
    List<AuthenticationProvider> providers = new ArrayList<>();

    if (securityProperties.getAuthentication().isEnabled()) {
      providers = authenticationProviders;
    }
    return new ProviderManager(providers);
  }

  @Bean
  @ConditionalOnExpression("'${feast.security.authentication.provider}' == 'jwt'")
  public AuthenticationProvider jwtAuthProvider() throws Exception {

    // Endpoint used to retrieve certificates to validate JWT token
    String jwkEndpointURI = "https://www.googleapis.com/oauth2/v3/certs";
    Map<String, String> options = securityProperties.getAuthentication().getOptions();
    // Provide a custom endpoint to retrieve certificates
    if (options != null) {
      jwkEndpointURI = options.get("jwkEndpointURI");
    }
    JwtAuthenticationProvider authProvider =
        new JwtAuthenticationProvider(NimbusJwtDecoder.withJwkSetUri(jwkEndpointURI).build());
    authProvider.setJwtAuthenticationConverter(new JwtAuthenticationConverter());
    return authProvider;
  }

  /**
   * Creates an AuthenticationReader that the AuthenticationManager will use to authenticate
   * requests
   *
   * @return GrpcAuthenticationReader
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  GrpcAuthenticationReader authenticationReader() {
    return new BearerAuthenticationReader(BearerTokenAuthenticationToken::new);
  }

  /**
   * Creates a SecurityMetadataSource when authentication is enabled. This allows for the
   * configuration of endpoint level security rules.
   *
   * @return GrpcSecurityMetadataSource
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authentication", name = "enabled")
  GrpcSecurityMetadataSource grpcSecurityMetadataSource() {
    final ManualGrpcSecurityMetadataSource source = new ManualGrpcSecurityMetadataSource();

    // Authentication is enabled for all gRPC endpoints
    source.setDefault(AccessPredicate.authenticated());

    // The following endpoints allow unauthenticated access
    source.set(CoreServiceGrpc.getGetFeastCoreVersionMethod(), AccessPredicate.permitAll());

    return source;
  }

  /**
   * Creates an AccessDecisionManager if authorization is enabled. This object determines the policy
   * used to make authorization decisions.
   *
   * @return AccessDecisionManager
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authorization", name = "enabled")
  AccessDecisionManager accessDecisionManager() {
    final List<AccessDecisionVoter<?>> voters = new ArrayList<>();
    voters.add(new AccessPredicateVoter());
    return new UnanimousBased(voters);
  }

  /**
   * Creates an AuthorizationProvider based on Feast configuration. This provider is available
   * through the security service.
   *
   * @return AuthorizationProvider used to validate access to Feast resources.
   */
  @Bean
  @ConditionalOnProperty(prefix = "feast.security.authorization", name = "enabled")
  AuthorizationProvider authorizationProvider() {
    if (securityProperties.getAuthentication().isEnabled()
        && securityProperties.getAuthorization().isEnabled()) {
      switch (securityProperties.getAuthorization().getProvider()) {
        case "KetoAuthorization":
          return new KetoAuthorizationProvider(securityProperties.getAuthorization().getOptions());
        default:
          throw new IllegalArgumentException(
              "Please configure an Authorization Provider if you have enabled authorization.");
      }
    }
    return null;
  }
}
