/**
 * Copyright 2009-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.javacrumbs.shedlock.provider.dynamodb.sync;

import static net.javacrumbs.shedlock.provider.dynamodb.sync.DynamoDBLockProvider.ID;
import static net.javacrumbs.shedlock.provider.dynamodb.sync.DynamoDBLockProvider.LOCKED_AT;
import static net.javacrumbs.shedlock.provider.dynamodb.sync.DynamoDBLockProvider.LOCKED_BY;
import static net.javacrumbs.shedlock.provider.dynamodb.sync.DynamoDBLockProvider.LOCK_UNTIL;
import static org.assertj.core.api.Assertions.assertThat;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.test.support.AbstractLockProviderIntegrationTest;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import cloud.localstack.TestUtils;
import cloud.localstack.docker.LocalstackDocker;
import cloud.localstack.docker.LocalstackDockerTestRunner;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.utils.ToString;

@RunWith(LocalstackDockerTestRunner.class)
// @RunWith(LocalstackTestRunner.class)
@LocalstackDockerProperties(randomizePorts = true)
public class DynamoDBLockProviderIntegrationTest extends AbstractLockProviderIntegrationTest {

    private static final String TABLE_NAME = "Shedlock";

    private static DynamoDbClient dynamoDbClient;

    @Before
    public void createLockProvider() {
        final ProvisionedThroughput provisionedThroughput =
            ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build();
        DynamoDBUtils.createLockTable(dynamoDbClient, TABLE_NAME, provisionedThroughput);
    }

    @After
    public void deleteLockTable() {
        final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder().tableName(TABLE_NAME).build();
        dynamoDbClient.deleteTable(deleteTableRequest);
    }

    @Override
    protected LockProvider getLockProvider() {
        return new DynamoDBLockProvider(dynamoDbClient, TABLE_NAME);
    }

    @Override
    protected void assertUnlocked(String lockName) {
        GetItemResponse lockItemResponse = getLockItem(lockName);
        assertThat(fromIsoString(lockItemResponse.getValueForField(LOCK_UNTIL, String.class).get()))
                .isBeforeOrEqualTo(now());
        assertThat(fromIsoString(lockItemResponse.getValueForField(LOCKED_AT, String.class).get()))
                .isBeforeOrEqualTo(now());
        assertThat(lockItemResponse.getValueForField(LOCKED_BY, String.class)).isNotEmpty();
    }

    private OffsetDateTime now() {
        return OffsetDateTime.now();
    }

    @Override
    protected void assertLocked(String lockName) {
        GetItemResponse lockItemResponse = getLockItem(lockName);
        assertThat(fromIsoString(lockItemResponse.getValueForField(LOCK_UNTIL, String.class).get())).isAfter(now());
        assertThat(fromIsoString(lockItemResponse.getValueForField(LOCKED_AT, String.class).get()))
                .isBeforeOrEqualTo(now());
        assertThat(lockItemResponse.getValueForField(LOCKED_BY, String.class)).isNotEmpty();
    }

    private OffsetDateTime fromIsoString(String isoString) {
        return OffsetDateTime.parse(isoString, DateTimeFormatter.ISO_DATE_TIME);
    }


    private GetItemResponse getLockItem(String lockName) {
        final HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(ID, AttributeValue.builder().s(lockName).build());

        final GetItemRequest itemRequest = GetItemRequest.builder().key(key).build();

        return dynamoDbClient.getItem(itemRequest);
    }

    @BeforeClass
    public static void startDynamoDB() throws URISyntaxException {

        final URI uri = new URI(LocalstackDocker.INSTANCE.getEndpointDynamoDB());

        dynamoDbClient =
            DynamoDbClient.builder().endpointOverride(uri).credentialsProvider(new InnerCredentialsProvider())
                .region(Region.SA_EAST_1).build();
    }

    @AfterClass
    public static void stopDynamoDB() {
    }

    static class InnerCredentialsProvider implements AwsCredentialsProvider {

        private InnerCredentialsProvider() {
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(TestUtils.TEST_ACCESS_KEY, TestUtils.TEST_SECRET_KEY);
        }

        @Override
        public String toString() {
            return ToString.create("InnerCredentialsProvider");
        }
    }
}
