package org.apigee.tutorial;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import java.nio.ByteBuffer;
import java.util.Map;

public class ColumnFamily {

  private final ExecutingKeyspace keyspace;
  private final String columnFamilyName;
  private final ColumnFamilyTemplate<ByteBuffer,DynamicComposite> columnFamilyTemplate;
  
  private static final DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();
  
  ColumnFamily(String columnFamilyName, ExecutingKeyspace keyspace) {
    this.columnFamilyName = columnFamilyName;
    this.keyspace = keyspace;    
    this.columnFamilyTemplate = 
      new ThriftColumnFamilyTemplate<ByteBuffer,DynamicComposite>(keyspace, 
          columnFamilyName, 
          ByteBufferSerializer.get(), 
          dcs);
  }
  
  public void insert(Row row) {
    Mutator<ByteBuffer> mutator = columnFamilyTemplate.createMutator();
    for (Map.Entry<ByteBuffer,HColumn<DynamicComposite,ByteBuffer>> entry : row.getColumns().entrySet() ) {
      HColumn<DynamicComposite, ByteBuffer> hColumn = entry.getValue();
      mutator.addInsertion(row.getKeyBytes(), columnFamilyName, hColumn);
      // insert new row in index cf with key: cfname_colname and colname: Composite(value, rowkey, timestamp)
      // key: rowkey_colname and colname: timestamp colvalue: value
      // indexingService.index(Mutator, HColumn, rowKey)
    }
    mutator.execute();
  }
  
  public CFCursor query(Row row) {
      // how are ranges handled in MBD? #query(Row, Row)
    CFCursor cursor;
    if ( !row.hasColumns() ) {
      cursor = new CFCursor(columnFamilyTemplate.queryColumns(row.getKeyBytes()));
    } else {
      cursor = new CFCursor(columnFamilyTemplate.queryColumns(row.getKeyBytes(),row.getColumnsForQuery()));
    }
    return cursor;
  }
}
