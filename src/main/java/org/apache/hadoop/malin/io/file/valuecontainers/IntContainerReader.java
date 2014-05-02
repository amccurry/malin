package org.apache.hadoop.malin.io.file.valuecontainers;

import org.apache.lucene.util.OpenBitSet;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class IntContainerReader {

  private final OpenBitSet _setValues;
  private final int _numberOfRecords;
  private final int[] _values;

  public IntContainerReader(int numberOfRecords, OpenBitSet setValues, int[] values) {
    _numberOfRecords = numberOfRecords;
    _setValues = setValues;
    _values = values;
  }

  public boolean isNull(int recordId) {
    return !_setValues.get(recordId);
  }

  public int get(int recordId) {
    return _values[recordId];
  }

  public int getNumberOfRecords() {
    return _numberOfRecords;
  }
}
