package org.apigee.tutorial;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.hector.api.beans.DynamicComposite;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class CFCursor implements Iterator<Row> {

  private final ColumnFamilyResult<ByteBuffer,DynamicComposite> columnFamilyResult;
  
  CFCursor(ColumnFamilyResult<ByteBuffer, DynamicComposite> columnFamilyResult) {
    this.columnFamilyResult = columnFamilyResult;    
  }
  
  @Override
  public boolean hasNext() {
    return columnFamilyResult.hasNext();
  }

  @Override
  public Row next() {   
    // CFR is already at 1st position, JDBC style
    Row row = new Row();    
    row.setKey(StringSerializer.get().fromByteBuffer(columnFamilyResult.getKey()));
    for (DynamicComposite columnName : columnFamilyResult.getColumnNames() ) {
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
  
}
