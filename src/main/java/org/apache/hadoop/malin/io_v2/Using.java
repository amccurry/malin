package org.apache.hadoop.malin.io_v2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DeflateCodec;

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

public class Using {

  public static void main(String[] args) throws IOException {
    ColumnWritableFactory columnWritableFactory = new ColumnWritableFactory();
    ColumnContainerFactory columnCollectorFactory = new ColumnContainerFactory();

    IntColumnContainer columnCollector1 = columnCollectorFactory.newInstance(IntType.class);
    IntColumnContainer columnCollector2 = columnCollectorFactory.newInstance(IntType.class);
    int maxRecord = 100000;
    boolean compress = false;
    Random random = new Random(0);
    for (int t = 0; t < 100; t++) {
      columnCollector1.reset();
      for (int i = 0; i < 10000; i++) {
        columnCollector1.set(random.nextInt(maxRecord), getRandomRange(random, 0, Integer.MAX_VALUE));
      }

      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();

      ColumnWritable<IntType> columnWritable = columnWritableFactory.newInstance(IntType.class);
      DeflateCodec defaultCodec = new DeflateCodec();

      if (compress) {
        defaultCodec.setConf(new Configuration());
        CompressionOutputStream outputStream = defaultCodec.createOutputStream(out);
        DataOutputStream dout = new DataOutputStream(outputStream);
        columnWritable.write(columnCollector1, dout);
        dout.close();
      } else {
        columnWritable.write(columnCollector1, out);
      }

      in.reset(out.getData(), out.getLength());

      if (compress) {
        CompressionInputStream inputStream = defaultCodec.createInputStream(in);
        DataInputStream din = new DataInputStream(inputStream);
        columnWritable.read(columnCollector2, din);
        din.close();
      } else {
        columnWritable.read(columnCollector1, in);
      }

      int[] values = columnCollector2.getValues();
      int numberOfNonNullValues = columnCollector2.getNumberOfNonNullValues();
      for (int i = 0; i < values.length && numberOfNonNullValues > 0; i++) {
        if (columnCollector2.isValueSet(i)) {
          // System.out.println("Record [" + i + "] Value [" + values[i] + "]");
          numberOfNonNullValues--;
        }
      }

      System.out.println("Size [" + out.getLength() + "]");
    }
  }

  private static int getRandomRange(Random random, int low, int high) {
    return random.nextInt(high - low) + low;
  }

}
