package org.apache.hadoop.malin.io_v2;

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

public class IntColumnContainer extends ColumnContainer<IntType> {

  private int[] _values = new int[1024];
  private int _minValue = Integer.MAX_VALUE;
  private int _maxValue = Integer.MIN_VALUE;
  private OpenBitSet _valuesThatAreSet = new OpenBitSet();

  public void set(int record, int value) {
    growIfNeeded(record);
    _values[record] = value;
    setMin(value);
    setMax(value);
    _valuesThatAreSet.set(record);
  }

  private void setMin(int value) {
    if (value < _minValue) {
      _minValue = value;
    }
  }

  private void setMax(int value) {
    if (value > _maxValue) {
      _maxValue = value;
    }
  }

  private void growIfNeeded(int record) {
    if (_values.length > record) {
      return;
    }
    int[] newValues = new int[record * 2];
    System.arraycopy(_values, 0, newValues, 0, _values.length);
    _values = newValues;
  }

  public int[] getValues() {
    return _values;
  }

  public int getMinValue() {
    return _minValue;
  }

  public int getMaxValue() {
    return _maxValue;
  }

  @Override
  public void reset() {
    _valuesThatAreSet = new OpenBitSet();
    _minValue = Integer.MAX_VALUE;
    _maxValue = Integer.MIN_VALUE;
  }
  
  public OpenBitSet getBitSet() {
    return _valuesThatAreSet;
  }

  @Override
  public int getNumberOfNonNullValues() {
    return (int) _valuesThatAreSet.cardinality();
  }

  @Override
  public boolean isValueSet(int record) {
    return _valuesThatAreSet.get(record);
  }

}
