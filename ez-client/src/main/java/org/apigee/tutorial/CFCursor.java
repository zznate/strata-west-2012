package org.apigee.tutorial;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.hector.api.beans.DynamicComposite;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class CFCursor implements Iterable<Row>,Iterator<Row> {

  private final ColumnFamilyResult<ByteBuffer,ByteBuffer> columnFamilyResult;
  private final ColumnFamily columnFamily;

  CFCursor(ColumnFamily columnFamily, ColumnFamilyResult<ByteBuffer, ByteBuffer> columnFamilyResult) {
    // change this to SliceQuery?
    this.columnFamilyResult = columnFamilyResult;
    this.columnFamily = columnFamily;
  }
  
  @Override
  public boolean hasNext() {
    return columnFamilyResult != null && columnFamilyResult.hasNext();
  }

  @Override
  public Row next() {   
    // CFR is already at 1st position, JDBC style
    Row row = new Row().apply(columnFamily);
    row.setKey(StringSerializer.get().fromByteBuffer(columnFamilyResult.getKey()));
    for (ByteBuffer columnName : columnFamilyResult.getColumnNames() ) {
      row.put(columnName, columnFamilyResult.getColumn(columnName));
    }
    if ( columnFamilyResult.hasNext() )
      columnFamilyResult.next();
    return row;
  }

  @Override
  public void remove() {
    columnFamilyResult.remove();    
  }

  @Override
  public Iterator<Row> iterator() {
    return this;
  }
}
