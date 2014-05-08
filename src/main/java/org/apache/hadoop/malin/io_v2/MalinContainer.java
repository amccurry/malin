package org.apache.hadoop.malin.io_v2;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.malin.io.file.valuecontainers.IntContainer;

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

public class MalinContainer {

  public static void main(String[] args) throws IOException {
    Path path = new Path("./local.bin");
    Configuration configuration = new Configuration();
    FileSystem fileSystem = path.getFileSystem(configuration);
    FSDataOutputStream out = fileSystem.create(path);
    Writer container = new MalinContainer.Writer(out);
    container.register(0L, new IntColumnContainer(), new IntColumnWritable());
    container.register(1L, new IntColumnContainer(), new IntColumnWritable());
    container.register(2L, new IntColumnContainer(), new IntColumnWritable());
    container.register(3L, new IntColumnContainer(), new IntColumnWritable());
    container.register(4L, new IntColumnContainer(), new IntColumnWritable());
    container.register(5L, new IntColumnContainer(), new IntColumnWritable());
    container.register(6L, new IntColumnContainer(), new IntColumnWritable());
    container.register(7L, new IntColumnContainer(), new IntColumnWritable());
    container.register(8L, new IntColumnContainer(), new IntColumnWritable());
    container.register(9L, new IntColumnContainer(), new IntColumnWritable());

    List<Column<? extends Type>> record = new ArrayList<Column<? extends Type>>();
    for (int i = 0; i < 100000; i++) {
      record.clear();
      for (int c = 0; c < 10; c++) {
        IntColumn column = new IntColumn(0);
        column.setValue(i + c);
        record.add(column);
      }
      container.append(record);
    }
    container.close();
  }

  public static class Writer implements Closeable {

    private final int _maxRecords = 10000;
    private final Map<Long, ColumnTuple> _columnMap = new TreeMap<Long, ColumnTuple>();
    private final FSDataOutputStream _out;
    private final ColumnPositions _columnPositions = new ColumnPositions();

    private int _recordId;

    public Writer(FSDataOutputStream out) {
      _out = out;
    }

    public void register(long globalColumnId, ColumnContainer<? extends Type> columnContainer,
        ColumnWritable<? extends Type> columnWritable) {
      _columnMap.put(globalColumnId, new ColumnTuple(globalColumnId, columnContainer, columnWritable));
    }

    public void append(List<Column<? extends Type>> record) throws IOException {
      int recordId = getRecordId();
      for (Column<? extends Type> c : record) {
        addColumn(recordId, c);
      }
      incrementRecord();
      tryToFlushSegment();
    }

    @Override
    public void close() throws IOException {
      long pos = _out.getPos();
      _columnPositions.write(_out);
      _out.writeLong(pos);
    }

    private void tryToFlushSegment() throws IOException {
      if (_recordId >= _maxRecords) {
        flush();
      }
    }

    private void flush() throws IOException {
      Map<Long, Long> columnToPosition = new TreeMap<Long, Long>();
      for (Entry<Long, ColumnTuple> e : _columnMap.entrySet()) {
        Long id = e.getKey();
        ColumnTuple tuple = e.getValue();
        ColumnContainer<Type> columnContainer = tuple.getColumnContainer();
        ColumnWritable<Type> columnWritable = tuple.getColumnWritable();
        long pos = _out.getPos();
        columnWritable.write(columnContainer, _out);
        columnToPosition.put(id, pos);
        columnContainer.reset();
      }
      _columnPositions.add(columnToPosition);
      _recordId = 0;
    }

    private void incrementRecord() {
      _recordId++;
    }

    private <T extends Type> void addColumn(int recordId, Column<T> column) throws IOException {
      ColumnContainer<T> container = getColumnContainer(column.getColumnId());
      column.add(recordId, container);
    }

    private int getRecordId() {
      return _recordId;
    }

    private <T extends Type> ColumnContainer<T> getColumnContainer(Long columnId) throws IOException {
      ColumnTuple columnTuple = _columnMap.get(columnId);
      if (columnTuple == null) {
        throw new IOException("Column container not found for columnId [" + columnId + "]");
      }
      ColumnContainer<T> columnContainer = columnTuple.getColumnContainer();
      return columnContainer;
    }

  }

  static class ColumnTuple {
    private final ColumnContainer<? extends Type> _columnContainer;
    private final ColumnWritable<? extends Type> _columnWritable;
    private final long _globalColumnId;

    ColumnTuple(long globalColumnId, ColumnContainer<? extends Type> columnContainer,
        ColumnWritable<? extends Type> columnWritable) {
      _columnContainer = columnContainer;
      _columnWritable = columnWritable;
      _globalColumnId = globalColumnId;
    }

    @SuppressWarnings("unchecked")
    <T extends Type> ColumnContainer<T> getColumnContainer() {
      return (ColumnContainer<T>) _columnContainer;
    }

    @SuppressWarnings("unchecked")
    <T extends Type> ColumnWritable<T> getColumnWritable() {
      return (ColumnWritable<T>) _columnWritable;
    }

    long getGlobalColumnId() {
      return _globalColumnId;
    }
  }

  static class ColumnPositions implements Writable {

    final List<Map<Long, Long>> _columnPositions = new ArrayList<Map<Long, Long>>();

    @Override
    public void write(DataOutput out) throws IOException {
      int size = _columnPositions.size();
      out.writeInt(size);
      for (int i = 0; i < size; i++) {
        Map<Long, Long> map = _columnPositions.get(i);
        out.writeInt(map.size());
        for (Entry<Long, Long> e : map.entrySet()) {
          out.writeLong(e.getKey());
          out.writeLong(e.getValue());
        }
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _columnPositions.clear();
      int size = in.readInt();
      for (int i = 0; i < size; i++) {
        Map<Long, Long> map = new TreeMap<Long, Long>();
        int c = in.readInt();
        for (int t = 0; t < c; t++) {
          map.put(in.readLong(), in.readLong());
        }
        _columnPositions.add(map);
      }
    }

    public void add(Map<Long, Long> columnToPosition) {
      _columnPositions.add(columnToPosition);
    }

  }
}
