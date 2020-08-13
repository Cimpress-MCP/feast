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

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudwatch.model.ResourceNotFoundException;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;

public class SnowflakeAuthorization {

  private AWSSecretsManager client;
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(SnowflakeAuthorization.class);
  private Client httpClient;

  public static final String CIMPRESS_OAUTH_URL = "<CIMPRESS_OAUTH_URL>";
  public static final String SNOWFLAKE_OAUTH_URL = "<SNOWFLAKE_OAUTH_URL>";

  public SnowflakeAuthorization() {
    AWSSecretsManagerClientBuilder clientBuilder =
        AWSSecretsManagerClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider())
            .withRegion("eu-west-1");
    this.client = clientBuilder.build();
    this.httpClient = Client.create();
  }

  private JsonNode getSecret(String secretName) {
    GetSecretValueRequest getSecretValueRequest =
        new GetSecretValueRequest().withSecretId(secretName);
    GetSecretValueResult getSecretValueResponse = null;
    try {
      getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
    } catch (ResourceNotFoundException e) {
      log.error("The requested secret " + secretName + " was not found");
    } catch (InvalidRequestException e) {
      log.error("The request was invalid due to: " + e.getMessage());
    } catch (InvalidParameterException e) {
      log.error("The request had invalid params: " + e.getMessage());
    }
    if (getSecretValueResponse == null) {
      return null;
    }
    String secret = getSecretValueResponse.getSecretString();
    ObjectMapper objectMapper = new ObjectMapper();
    if (secret != null) {
      try {
        return objectMapper.readTree(secret);
      } catch (IOException e) {
        log.error("Exception while retreiving secret values: " + e.getMessage());
      }
    } else {
      log.error("The Secret String returned is null");
      return null;
    }
    return null;
  }

  public String[] getSnowFlakeCredentials(String awsSecretName, String OauthClientId)
      throws JsonProcessingException, IOException {
    JsonNode oauthCred = this.getSecret(awsSecretName);
    String oauthClientSecret = oauthCred.get(OauthClientId).textValue();
    String jwt = getJwt(OauthClientId, oauthClientSecret);
    String response =
        this.httpClient
            .resource(SNOWFLAKE_OAUTH_URL)
            .header("accept", "application/json")
            .header("authorization", "Bearer " + jwt)
            .accept("application/json")
            .post(String.class);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(response).get("snowflake");
    String[] auth = new String[2];
    auth[0] = String.format(json.get("username").textValue());
    auth[1] = String.format(json.get("password").textValue());
    return auth;
  }

  private String getJwt(String oauthClientId, String oauthClientSecret)
      throws JsonProcessingException, IOException {
    Map<String, String> data = new HashMap<String, String>();
    data.put("grant_type", "client_credentials");
    data.put("client_id", oauthClientId);
    data.put("client_secret", oauthClientSecret);
    data.put("audience", "https://api.cimpress.io/");
    JSONObject jsonData = new JSONObject(data);

    String response =
        this.httpClient
            .resource(CIMPRESS_OAUTH_URL)
            .header("content-type", "application/json")
            .accept("application/json")
            .post(String.class, jsonData.toString());
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readTree(response).get("access_token").textValue();
  }

  //	public static void main(String[] args) throws Exception {
  //		SnowflakeAuthorization auth = new SnowflakeAuthorization();
  //		System.out.println(auth.getSnowFlakeCredentials(OAUTH_CLIENT_SECRET_NAME,
  //				OAUTH_CLIENT_ID));
  //	}

}
