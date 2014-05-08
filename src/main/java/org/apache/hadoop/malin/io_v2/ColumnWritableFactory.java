package org.apache.hadoop.malin.io_v2;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

public class ColumnWritableFactory {

  private Map<Class<? extends Type>, Class<? extends ColumnWritable<?>>> _implMap = null;

  public ColumnWritableFactory() {
    _implMap = new ConcurrentHashMap<>();
    _implMap.put(IntType.class, IntColumnWritable.class);
  }

  @SuppressWarnings("unchecked")
  public <TYPE extends Type, WRITABLE extends ColumnWritable<TYPE>> WRITABLE newInstance(Class<TYPE> typeClass)
      throws IOException {
    Class<WRITABLE> impl = (Class<WRITABLE>) _implMap.get(typeClass);
    if (impl == null) {
      throw new IOException("Type [" + typeClass + "] not found.");
    }
    try {
      return (WRITABLE) impl.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
