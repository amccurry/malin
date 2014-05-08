package org.apache.hadoop.malin.io_v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.file.tfile.Utils;
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

public class IntColumnWritable extends ColumnWritable<IntType> {

  @Override
  public void write(ColumnContainer<IntType> container, DataOutput out) throws IOException {
    IntColumnContainer intContainer = (IntColumnContainer) container;

    OpenBitSet bitSet = intContainer.getBitSet();
    int numWords = bitSet.getNumWords();
    long[] bits = bitSet.getBits();
    Utils.writeVInt(out, numWords);
    for (int i = 0; i < numWords; i++) {
      out.writeLong(bits[i]);
    }

    int numberOfNonNullValues = intContainer.getNumberOfNonNullValues();
    int index = -1;
    int[] values = intContainer.getValues();
    out.writeInt(numberOfNonNullValues);
    for (int i = 0; i < numberOfNonNullValues; i++) {
      index = bitSet.nextSetBit(index + 1);
      if (index < -1) {
        throw new IOException("Something went wrong.");
      }
      out.writeInt(values[index]);
    }
  }

  @Override
  public void read(ColumnContainer<IntType> container, DataInput in) throws IOException {
    IntColumnContainer intContainer = (IntColumnContainer) container;
    intContainer.reset();
    int numWords = Utils.readVInt(in);
    long[] bits = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      bits[i] = in.readLong();
    }
    OpenBitSet bitSet = new OpenBitSet(bits, numWords);
    int numberOfNonNullValues = in.readInt();
    int index = -1;
    for (int i = 0; i < numberOfNonNullValues; i++) {
      index = bitSet.nextSetBit(index + 1);
      if (index < -1) {
        throw new IOException("Something went wrong.");
      }
      intContainer.set(index, in.readInt());
    }
  }

}
