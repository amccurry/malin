package org.apache.hadoop.malin.io_v2.types;

import org.apache.hadoop.malin.io_v2.ColumnContainer;
import org.apache.hadoop.malin.io_v2.ColumnWritable;
import org.apache.hadoop.malin.io_v2.Type;

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

public class LongType extends Type {

  @Override
  public ColumnContainer<? extends Type> getColumnCollector() {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public ColumnWritable<? extends Type> getColumnWritable() {
    throw new RuntimeException("Not Implemented");
  }

}
