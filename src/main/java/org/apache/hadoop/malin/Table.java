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
package org.apache.hadoop.malin;

import java.io.IOException;
import java.util.List;

public abstract class Table {

  /**
   * Gets the {@link Column} names for this table.
   * 
   * @return the {@link List} of the {@link Column} names.
   * @throws IOException
   */
  public abstract List<String> getColumnNames() throws IOException;

  /**
   * Fetches {@link Row} by a {@link RowId}.
   * 
   * @param rowId
   *          the {@link RowId}.
   * @return the {@link Row} if found, null if missing.
   * @throws IOException
   */
  public abstract Row getRow(RowId rowId) throws IOException;

  /**
   * Adds a {@link Row} and returns a new {@link RowId} for the given
   * {@link Row}.
   * 
   * @param row
   *          the {@link Row} to be added.
   * @return the new {@link RowId}.
   * @throws IOException
   */
  public abstract RowId addRow(Row row) throws IOException;

  /**
   * Deletes the given {@link RowId}.
   * 
   * @param rowId
   *          the {@link RowId}.
   * @throws IOException
   */
  public abstract void delete(RowId rowId) throws IOException;

  /**
   * Gets a {@link TableIndex} by name.
   * 
   * @param name
   *          the name of the index.
   * @return the {@link TableIndex}.
   * @throws IOException
   */
  public abstract TableIndex getTableIndex(String name) throws IOException;

  /**
   * Creates a new {@link Row}.
   * 
   * @return the new {@link Row}
   * @throws IOException
   */
  public abstract Row createRow() throws IOException;

  /**
   * Creates a new {@link Column} to be add to a {@link Row}.
   * 
   * @param name
   *          the name of the {@link Column}.
   * @return
   * @throws IOException
   */
  public abstract <T> Column<T> createColumn(String name) throws IOException;

}
