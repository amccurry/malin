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

public class SegmentWriter implements Closeable {
  
  private final SegmentDataWriter _segmentDataWriter;
  private final List<SegmentInspector> _segmentInspectors;
  private final SegmentMetaData _segmentMetaData;
  
  private int _count;

  public SegmentWriter(SegmentMetaData segmentMetaData, SegmentDataWriter segmentDataWriter,
      List<SegmentInspector> segmentInspectors) {
    _segmentMetaData = segmentMetaData;
    _segmentDataWriter = segmentDataWriter;
    _segmentInspectors = segmentInspectors;
  }

  @Override
  public void close() throws IOException {
    _segmentDataWriter.close();
    for (SegmentInspector inspector : _segmentInspectors) {
      inspector.close();
    }
    _segmentMetaData.close();
  }

  public void append(MalinRecord malinRecord) {
    _segmentDataWriter.append(malinRecord);
    for (SegmentInspector inspector : _segmentInspectors) {
      inspector.inspect(malinRecord);
    }
    _count++;
  }

  public int size() {
    return _count;
  }
}
