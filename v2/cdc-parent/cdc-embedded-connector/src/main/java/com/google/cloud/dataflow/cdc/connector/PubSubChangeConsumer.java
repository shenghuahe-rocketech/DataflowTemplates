/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.connector;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils.DataCatalogSchemaManager;
import com.google.cloud.dataflow.cdc.common.ObjectHelper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.EmbeddedEngine.RecordCommitter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Implements Debezium's Embedded Engine change consumer to push data to PubSub. */
public class PubSubChangeConsumer implements EmbeddedEngine.ChangeConsumer {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PubSubChangeConsumer.class);

  private static final long requestBytesThreshold = 1048576L; // default : 1 byte
  private static final long messageCountBatchSize = 1000L; // default : 1 message
  private static final Duration publishDelayThreshold = Duration.ofMillis(1000); // default : 1 ms

  private static final BatchingSettings batchingSettings =
      BatchingSettings.newBuilder()
          .setElementCountThreshold(messageCountBatchSize)
          .setRequestByteThreshold(requestBytesThreshold)
          .setDelayThreshold(publishDelayThreshold)
          .build();

  public static final BiFunction<String, DataCatalogSchemaManager, Publisher>
      DEFAULT_PUBLISHER_FACTORY = (tableName, schemaUtils) -> {
    try {
      ProjectTopicName projectTopicName = ProjectTopicName.of(
          schemaUtils.getGcpProject(), schemaUtils.getPubSubTopicForTable(tableName));
      return Publisher
          .newBuilder(projectTopicName)
          .setBatchingSettings(batchingSettings)
          .build();
    } catch (IOException e) {
      LOG.error("Unable to create a PubSub Publisher", e);
      return null;
    }
  };

  private final Map<String, Publisher> pubsubPublisherMap;

  private final Set<String> whitelistedTables;
  private final Set<String> observedTables;
  private final DataCatalogSchemaManager schemaUpdater;
  private final BiFunction<String, DataCatalogSchemaManager, Publisher>  pubSubPublisherFactory;
  private final DebeziumSourceRecordToDataflowCdcFormatTranslator translator =
      new DebeziumSourceRecordToDataflowCdcFormatTranslator();

  public PubSubChangeConsumer(
      Set<String> whitelistedTables,
      DataCatalogSchemaManager schemaUpdater,
      BiFunction<String, DataCatalogSchemaManager, Publisher> pubSubPublisherFactory) {
    this.whitelistedTables = whitelistedTables;
    this.observedTables = new HashSet<>();
    this.pubsubPublisherMap = new HashMap<>();
    this.schemaUpdater = schemaUpdater;
    this.pubSubPublisherFactory = pubSubPublisherFactory;
  }

  private Publisher getPubSubPublisher(String tableName) {
    if (!pubsubPublisherMap.containsKey(tableName)) {
      Publisher result = pubSubPublisherFactory.apply(tableName, schemaUpdater);
      pubsubPublisherMap.put(tableName, result);
      return result;
    }

    return pubsubPublisherMap.get(tableName);
  }

  @Override
  public void handleBatch(
      List<SourceRecord> records, RecordCommitter committer) throws InterruptedException {

    ImmutableList.Builder<ApiFuture<String>> futureListBuilder = ImmutableList.builder();

    Set<Publisher> usedPublishers = new HashSet<>();

    int recordCount = 0;
    long start = System.nanoTime();

    // TODO(pabloem): Improve the commit logic.
    for (SourceRecord r : records) {

      recordCount++;

      // Debezium publishes updates for each table in a separate Kafka topic, which is the fully
      // qualified name of the MySQL table (e.g. dbInstanceName.databaseName.table_name).
      String tableName = r.topic();

      if (whitelistedTables.contains(tableName)) {
        Row updateRecord = translator.translate(r);
        if (updateRecord == null) {
          continue;
        }

        if (!observedTables.contains(tableName)) {
          LOG.info("Updating schema for observed table: " + tableName);
          Entry result = schemaUpdater.updateSchemaForTable(
              tableName, updateRecord.getSchema());
          if (result == null) {
            throw new InterruptedException(
                "A problem occurred when communicating with Cloud Data Catalog");
          }
          observedTables.add(tableName);
        }

        Publisher pubSubPublisher = this.getPubSubPublisher(tableName);
        if (pubSubPublisher == null) {
          // We were unable to create a pubSubPublisher for this topic. This is bad, and we should
          // stop execution without committing any more messages.
          throw new InterruptedException("Unable to create a PubSub topic for table " + tableName);
        }
        usedPublishers.add(pubSubPublisher);

        PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
        LOG.debug("Update Record is: {}", updateRecord);

        // always get the latest schema from the record because it could have been changed
        byte[] record = ObjectHelper.convertToByteArray(updateRecord);

        if (record != null) {
          byte[] recordCompressed = ObjectHelper.gzipCompress(record);
          ByteString data = ByteString.copyFrom(recordCompressed);

          PubsubMessage message = messageBuilder
                  .setData(data)
                  .putAttributes("table", tableName)
                  .build();
          futureListBuilder.add(pubSubPublisher.publish(message));
        }

      } else {
        LOG.debug("Discarding record: {}", r);
      }
      committer.markProcessed(r);
    }

    usedPublishers.forEach(p -> p.publishAllOutstanding());

    for (ApiFuture<String> f : futureListBuilder.build()) {
      try {
        String result = f.get();
        LOG.debug("Result from PubSub Publish Future: {}", result);
      } catch (ExecutionException e) {
        LOG.error("Exception when executing future {}: {}. Stopping execution.", f, e);
        return;
      }
    }

    committer.markBatchFinished();

    long elapsedTime = System.nanoTime() - start;
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    LOG.info("Processed batch of: " + recordCount + " in " + elapsedTime / 1000000 + " ms, at " + dateFormat.format(date));
  }
}
