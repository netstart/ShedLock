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
 *
 * Contributors:
 *
 * Clayton K. N. Passos
 */
package net.javacrumbs.shedlock.provider.dynamodb.sync;

import static net.javacrumbs.shedlock.provider.dynamodb.sync.DynamoDBLockProvider.ID;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class DynamoDBUtils {

    /**
     * Creates a locking table with the given name.
     * <p>
     * This method does not check if a table with the given name exists already.
     *
     * @param dynamoDBClient DynamoDBCliet to be used
     * @param tableName table to be used
     * @param throughput AWS {@link ProvisionedThroughput throughput requirements} for the given lock
     *        setup
     * @return a {@link DynamoDbClient }
     *
     */
    public static DynamoDbClient createLockTable(DynamoDbClient dynamoDBClient, String tableName,
                                                 ProvisionedThroughput throughput) {
        final CreateTableRequest request = createTableRequest(tableName, throughput);
        final CreateTableResponse response = dynamoDBClient.createTable(request);
        return dynamoDBClient;
    }

    private static CreateTableRequest createTableRequest(final String tableName,
                                                         final ProvisionedThroughput throughput) {
        return CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(keySchemaElement())
                .attributeDefinitions(attributeDefinition())
                .provisionedThroughput(throughput)
                .build();
    }

    private static AttributeDefinition attributeDefinition() {
        return AttributeDefinition.builder().attributeName(ID).attributeType(ScalarAttributeType.S).build();
    }

    private static KeySchemaElement keySchemaElement() {
        return KeySchemaElement.builder().attributeName(ID).keyType(KeyType.HASH).build();
    }
}
