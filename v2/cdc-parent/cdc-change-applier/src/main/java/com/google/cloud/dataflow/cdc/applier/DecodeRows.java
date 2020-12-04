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
package com.google.cloud.dataflow.cdc.applier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Optional;

import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import com.google.cloud.dataflow.cdc.common.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Receives byte-encoded Rows, and returns decoded Row objects.
 */
public class DecodeRows extends PTransform<PCollection<byte[]>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(DecodeRows.class);

  private DecodeRows() {
  }

  public static DecodeRows decode() {
    return new DecodeRows();
  }

  public PCollection<Row> expand(PCollection<byte[]> input) {
    return input.apply(MapElements.into(TypeDescriptors.rows())
        .via(elm -> {
          String base64EncodedRecord = ByteString.copyFrom(elm).toStringUtf8();
          LOG.debug("Row decoded: " + base64EncodedRecord);
          Optional<Serializable> payload = ObjectHelper.convertFrom(base64EncodedRecord);

          return (Row) payload.orElse(null);
        }));
  }
}
