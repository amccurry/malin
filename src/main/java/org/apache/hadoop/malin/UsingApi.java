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

public class UsingApi {

  public static void main(String[] args) throws IOException {
    DatabaseManager databaseManager = getDatabaseManager();

    {
      // Creates the database.
      databaseManager.createDatabase("test");
      AlterDatabase alterDatabase = databaseManager.alterDatabase("test");
      alterDatabase.createTable("mytable");
      AlterTable alterTable = alterDatabase.alterTable("mytable");

      // The col def stuff is not fully thought out...

      alterTable.addColumn(new ColumnDefintion()); // col_1
      alterTable.addColumn(new ColumnDefintion()); // col_2
      alterTable.addColumn(new ColumnDefintion()); // col_3
    }

    RowId rowId;
    {
      DatabaseTransaction t = databaseManager.startTransaction("test");
      Table table = t.getTable("mytable");
      Row row = table.createRow();
      row.addColumn(table.createColumn("col1").setValue(1234));
      row.addColumn(table.createColumn("col2").setValue("string"));
      row.addColumn(table.createColumn("col3").setValue(0.34d));
      rowId = table.addRow(row);
      t.commit();
    }

    {
      DatabaseTransaction t = databaseManager.startTransaction("test");
      Table table = t.getTable("mytable");
      Row row = table.getRow(rowId);
      System.out.println(row);
      t.commit();
    }

    {
      DatabaseTransaction t = databaseManager.startTransaction("test");
      Table table = t.getTable("mytable");
      TableIndex tableIndex = table.getTableIndex("index1");
      List<TableIndexPartition> partitions = tableIndex.getPartitions();
      // The index partitions could be used to process indexes in parallel.
      for (TableIndexPartition indexPartition : partitions) {
        RowId id;
        while ((id = indexPartition.nextRow()) != RowId.NO_MORE) {
          Row row = table.getRow(id);
          System.out.println(row);
        }
      }
      t.commit();
    }

    {
      DatabaseTransaction t = databaseManager.startTransaction("test");
      Table table = t.getTable("mytable");
      table.delete(rowId);
      t.commit();
    }

  }

  private static DatabaseManager getDatabaseManager() {
    return null;
  }

}
