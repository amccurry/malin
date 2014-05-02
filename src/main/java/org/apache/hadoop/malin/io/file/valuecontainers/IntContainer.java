package org.apache.hadoop.malin.io.file.valuecontainers;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.malin.Writable;
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

public class IntContainer implements Writable<IntContainerReader>, Closeable, Container {

  private boolean _closed;
  private MetaData _metaData;
  private COMPRESSION_TYPE _compressionType;
  private int[] _values;
  private int _numberOfRecords;
  private OpenBitSet _setValues;
  private int _minValue = Integer.MAX_VALUE;
  private int _maxValue = Integer.MIN_VALUE;

  public IntContainer() {

  }

  public IntContainer(int defaultNumberOfRecords) {
    _values = new int[defaultNumberOfRecords];
    _setValues = new OpenBitSet();
  }

  public enum COMPRESSION_TYPE {
    NONE((byte) 0), NARROW((byte) 1), NARROW_SPARSE((byte) 2);

    private final byte _b;

    private COMPRESSION_TYPE(byte b) {
      _b = b;
    }

    public void write(DataOutput out) throws IOException {
      out.write(_b);
    }

    public static COMPRESSION_TYPE read(DataInput in) throws IOException {
      byte b = in.readByte();
      switch (b) {
      case 0:
        return NONE;
      case 1:
        return NARROW;
      case 2:
        return NARROW_SPARSE;
      default:
        throw new IOException("Not supported type [" + b + "]");
      }
    }
  }

  public void set(int recordId, int value) throws IOException {
    checkIfOpen();
    growIfNeeded(recordId);
    _values[recordId] = value;
    _setValues.set(recordId);
    setMinValue(value);
    setMaxValue(value);
    setMaxRecord(recordId);
  }

  private void setMaxRecord(int recordId) {
    if (recordId >= _numberOfRecords) {
      _numberOfRecords = recordId + 1;
    }
  }

  @Override
  public void close() throws IOException {
    if (_closed) {
      throw new IOException("Already closed!");
    }
    _closed = true;
    createMetaData();
  }

  private void createMetaData() {
    _metaData = new MetaData(IntContainer.class);
    _compressionType = COMPRESSION_TYPE.NONE;
    _metaData.add(MetaDataType.DATA_COMPRESSION, _compressionType);
  }

  @Override
  public MetaData getMetaData() {
    return _metaData;
  }

  @Override
  public IntContainerReader read(DataInput in) throws IOException {
    COMPRESSION_TYPE compressionType = COMPRESSION_TYPE.read(in);
    switch (compressionType) {
    case NONE:
      int numWords = in.readInt();
      long[] bits = new long[numWords];
      for (int i = 0; i < numWords; i++) {
        bits[i] = in.readLong();
      }
      OpenBitSet setValues = new OpenBitSet();
      setValues.setBits(bits);
      setValues.setNumWords(numWords);
      int numberOfRecords = in.readInt();
      int[] values = new int[numberOfRecords];
      for (int i = 0; i < numberOfRecords; i++) {
        if (setValues.get(i)) {
          values[i] = in.readInt();
        }
      }
      return new IntContainerReader(numberOfRecords, setValues, values);
    default:
      throw new IOException("Compression type [" + compressionType + "] not supported.");
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    checkIfClosed();
    _compressionType.write(out);
    switch (_compressionType) {
    case NONE:
      int numWords = _setValues.getNumWords();
      long[] bits = _setValues.getBits();
      out.writeInt(numWords);
      for (int i = 0; i < numWords; i++) {
        out.writeLong(bits[i]);
      }
      out.writeInt(_numberOfRecords);
      for (int i = 0; i < _numberOfRecords; i++) {
        if (_setValues.get(i)) {
          out.writeInt(_values[i]);
        }
      }
      return;
    default:
      throw new IOException("Compression type [" + _compressionType + "] not supported.");
    }
  }

  private void checkIfClosed() throws IOException {
    if (!_closed) {
      throw new IOException("Not Closed!");
    }
  }

  private void setMaxValue(int value) {
    if (value < _minValue) {
      _minValue = value;
    }
  }

  private void setMinValue(int value) {
    if (value < _maxValue) {
      _maxValue = value;
    }
  }

  private void growIfNeeded(int recordId) {
    if (recordId >= _values.length) {
      int[] newValues = new int[recordId * 2];
      System.arraycopy(_values, 0, newValues, 0, _values.length);
      _values = newValues;
    }
  }

  private void checkIfOpen() throws IOException {
    if (_closed) {
      throw new IOException("Closed!");
    }
  }

}
