package org.apache.hadoop.malin.io.file.valuecontainers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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

public class UsingIntContainer {

  public static void main(String[] args) throws IOException {
    IntContainer intContainer = new IntContainer(1000);
    intContainer.set(300, 4000);
    intContainer.close();
    
    
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    intContainer.write(dataOutputStream);
    
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    IntContainer readerContianer = new IntContainer();
    IntContainerReader reader = readerContianer.read(new DataInputStream(inputStream));

    for (int i = 0; i < reader.getNumberOfRecords(); i++) {
      if (!reader.isNull(i)) {
        System.out.println(reader.get(i));
      }
    }
    
  }

}
