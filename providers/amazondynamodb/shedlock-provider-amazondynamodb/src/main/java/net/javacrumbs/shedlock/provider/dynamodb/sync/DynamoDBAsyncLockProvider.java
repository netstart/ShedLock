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
package com.deliverypf.fleet.telemetryorder.configs.dynamodb.lockprovider;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.Utils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.javacrumbs.shedlock.support.Utils.toIsoString;

/**
 * Distributed lock using DynamoDB. Depends on <code>aws-java-sdk-dynamodb</code>.
 * <p>
 * It uses a dynamoDbClient with the following structure:
 * 
 * <pre>
 * {
 *    "_id" : "lock name",
 *    "lockUntil" : ISODate("2017-01-07T16:52:04.071Z"),
 *    "lockedAt" : ISODate("2017-01-07T16:52:03.932Z"),
 *    "lockedBy" : "host name"
 * }
 * </pre>
 *
 * <code>lockedAt</code> and <code>lockedBy</code> are just for troubleshooting and are not read by
 * the code.
 *
 * <ol>
 * <li>Attempts to insert a new lock record.</li>
 * <li>We will try to update lock record using
 * <code>filter _id == :name AND lock_until &lt;= :now</code>.</li>
 * <li>If the update succeeded, we have the lock. If the update failed (condition check exception)
 * somebody else holds the lock.</li>
 * <li>When unlocking, <code>lock_until</code> is set to <i>now</i> or <i>lockAtLeastUntil</i>
 * whichever is later.</li>
 * </ol>
 *
 */
public class DynamoDBAsyncLockProvider implements LockProvider {
    static final String LOCK_UNTIL = "lockUntil";
    static final String LOCKED_AT = "lockedAt";
    static final String LOCKED_BY = "lockedBy";
    static final String ID = "_id";

    private static final String OBTAIN_LOCK_QUERY =
        "set " + LOCK_UNTIL + " = :lockUntil, " + LOCKED_AT + " = :lockedAt, " + LOCKED_BY + " = :lockedBy";
    private static final String OBTAIN_LOCK_CONDITION =
        LOCK_UNTIL + " <= :lockedAt or attribute_not_exists(" + LOCK_UNTIL + ")";
    private static final String RELEASE_LOCK_QUERY = "set " + LOCK_UNTIL + " = :lockUntil";

    private final String hostname;
    private final DynamoDbAsyncClient dynamoDbClient;
    private final String tableName;

    /**
     * Uses DynamoDB to coordinate locks
     *
     * @param dynamoDBClient
     */
    public DynamoDBAsyncLockProvider(DynamoDbAsyncClient dynamoDBClient, String tableName) {
        this.dynamoDbClient = dynamoDBClient;
        this.tableName = tableName;
        this.hostname = Utils.getHostname();
    }

    @Override
    public Optional<SimpleLock> lock(LockConfiguration lockConfiguration) {
        String nowIso = toIsoString(now());
        String lockUntilIso = toIsoString(lockConfiguration.getLockAtMostUntil());

        final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                .key(key(ID, lockConfiguration.getName()))
                .updateExpression(OBTAIN_LOCK_QUERY)
                .conditionExpression(OBTAIN_LOCK_CONDITION)
                .expressionAttributeValues(expressionAttributeValues(nowIso, lockUntilIso))
                .returnValues(ReturnValue.UPDATED_NEW)
                .build();

        try {
            final CompletableFuture<String> updateItem = dynamoDbClient.updateItem(updateItemRequest)
                    .thenApply(e -> e.getValueForField(LOCK_UNTIL, String.class).get());

            final String lock = updateItem.get(5, TimeUnit.SECONDS);

            assert lockUntilIso.equals(lock);
            return Optional.of(new DynamoDBLock(lockConfiguration));

        } catch (ConditionalCheckFailedException | InterruptedException | ExecutionException | TimeoutException e) {
            return Optional.empty();
        }
    }

    private HashMap<String, AttributeValue> key(final String id, final String name) {
        final HashMap<String, AttributeValue> key = new HashMap<>();
        key.put(id, attribuiteValueAsString(name));
        return key;
    }

    private HashMap<String, AttributeValue> expressionAttributeValues(final String nowIso, final String lockUntilIso) {
        final HashMap<String, AttributeValue> values = new HashMap<>();
        values.put(":lockUntil", attribuiteValueAsString(lockUntilIso));
        values.put(":lockedAt", attribuiteValueAsString(nowIso));
        values.put(":lockedBy", attribuiteValueAsString(hostname));
        return values;
    }

    private AttributeValue attribuiteValueAsString(String s) {
        return AttributeValue.builder().s(s).build();
    }

    private Instant now() {
        return Instant.now();
    }


    private final class DynamoDBLock implements SimpleLock {
        private final LockConfiguration lockConfiguration;

        private DynamoDBLock(LockConfiguration lockConfiguration) {
            this.lockConfiguration = lockConfiguration;
        }

        @Override
        public void unlock() {
            String unlockTimeIso = toIsoString(lockConfiguration.getUnlockTime());

            final HashMap<String, AttributeValue> values = new HashMap<>();
            values.put(":lockUntil", attribuiteValueAsString(unlockTimeIso));

            final UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .key(key(ID, lockConfiguration.getName()))
                    .updateExpression(RELEASE_LOCK_QUERY)
                    .expressionAttributeValues(values)
                    .returnValues(ReturnValue.UPDATED_NEW)
                    .build();

            final CompletableFuture<String> result = dynamoDbClient.updateItem(updateItemRequest)
                    .thenApply(e -> e.getValueForField(LOCK_UNTIL, String.class).get());

            try {
                final String lock = result.get(5, TimeUnit.SECONDS);
                assert unlockTimeIso.equals(lock);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
