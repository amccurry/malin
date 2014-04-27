/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.malin.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class MalinContainer {

  private MalinContainer() {

  }

  public static void main(String[] args) throws IOException {
    Writer writer = new Writer(null);
    writer.appendRecord(new Object[] { "cool" });
    writer.close();
  }

  public static class Reader {
  }

  public static class Writer implements Closeable {

    private final int _maxSegmentSize;
    private final MalinCodec _codec;
    private final RecordConvertor _recordConvertor;
    private final ContainerMetaData _containerMetaData;
    private final List<ContainerInspector> _containerInspectors;

    private SegmentWriter _segmentWriter;

    public Writer(ContainerMetaData containerMetaData) throws IOException {
      _codec = null;// @TODO new MalinCodec();
      _containerMetaData = containerMetaData;
      _maxSegmentSize = _containerMetaData.getMaxSegmentRecordSize();
      _recordConvertor = _codec.getRecordConvertor(_containerMetaData);
      _containerInspectors = _codec.getContainerInspectors(_containerMetaData);
      _segmentWriter = openSegmentForWriting(_containerMetaData);
    }

    public void appendRecord(Object[] record) throws IOException {
      tryToCloseSegment();
      MalinRecord malinRecord = _recordConvertor.convert(record);
      _segmentWriter.append(malinRecord);
      for (ContainerInspector inspector : _containerInspectors) {
        inspector.inspect(malinRecord);
      }
    }

    private void tryToCloseSegment() throws IOException {
      if (shouldCloseSegment()) {
        closeSegmentForWriting();
        _segmentWriter = openSegmentForWriting(_containerMetaData);
      }
    }

    private SegmentWriter openSegmentForWriting(ContainerMetaData containerMetaData) throws IOException {
      SegmentMetaData segmentMetaData = _codec.newSegmentMetaData(containerMetaData);
      SegmentDataWriter segmentDataWriter = _codec.getSegmentWriter(segmentMetaData);
      List<SegmentInspector> segmentInspectors = _codec.getSegmentInspectors(segmentMetaData);
      return new SegmentWriter(segmentMetaData, segmentDataWriter, segmentInspectors);
    }

    private void closeSegmentForWriting() throws IOException {
      _segmentWriter.close();
      _segmentWriter = null;
    }

    private boolean shouldCloseSegment() {
      if (_segmentWriter.size() >= _maxSegmentSize) {
        return true;
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      closeSegmentForWriting();
      for (ContainerInspector inspector : _containerInspectors) {
        inspector.close();
      }
      _containerMetaData.close();
    }
  }
}
