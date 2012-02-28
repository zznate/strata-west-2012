package org.apigee.tutorial;

import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.Operation;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.template.ColumnFamilyResultWrapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ColumnFamily {

  private final ExecutingKeyspace keyspace;
  private final String columnFamilyName;
  private final ColumnFamilyTemplate<ByteBuffer,ByteBuffer> columnFamilyTemplate;
  private final ConcurrentHashMap extensions = new ConcurrentHashMap();
  
  ColumnFamily(String columnFamilyName, ExecutingKeyspace keyspace) {
    // TODO add CfMetaData poll in c-tor
    this.columnFamilyName = columnFamilyName;
    this.keyspace = keyspace;    
    this.columnFamilyTemplate = 
      new ThriftColumnFamilyTemplate<ByteBuffer,ByteBuffer>(keyspace,
          columnFamilyName, 
          ByteBufferSerializer.get(), 
          ByteBufferSerializer.get());
  }

  public void delete(Row row) {
    columnFamilyTemplate.deleteRow(row.getKeyBytes());
  }

  public void delete(Iterable<Row> rows) {
    for (Row row : rows ) {
      delete(row);
    }
  }
  
  public void insert(Row row) {
    // TODO if row.hasCounters(); add to counter CF
    Mutator<ByteBuffer> mutator = columnFamilyTemplate.createMutator();
    for (Map.Entry<ByteBuffer,HColumn<ByteBuffer,ByteBuffer>> entry : row.getColumns().entrySet() ) {
      HColumn<ByteBuffer, ByteBuffer> hColumn = entry.getValue();
      mutator.addInsertion(row.getKeyBytes(), columnFamilyName, hColumn);
      // insert new row in index cf with key: cfname_colname and colname: Composite(value, rowkey, timestamp)
      // key: rowkey_colname and colname: timestamp colvalue: value
      // indexingService.index(Mutator, HColumn, rowKey)
    }
    if ( row.hasCounters() ) {
      for ( Map.Entry<ByteBuffer, HCounterColumn<ByteBuffer>> entry : row.getCounters().entrySet() ) {
        mutator.addCounter(row.getKeyBytes(), org.apigee.tutorial.Cassandra.COUNTER_CF_NAME, entry.getValue());
      }
    }
    mutator.execute();
  }
  
  public CFCursor query(Row row) {
      // TODO add test case
    CFCursor cursor;
    if ( !row.hasColumns() ) {
      cursor = new CFCursor(this,columnFamilyTemplate.queryColumns(row.getKeyBytes()));
    } else {
      cursor = new CFCursor(this,columnFamilyTemplate.queryColumns(row.getKeyBytes(),row.getColumnsForQuery()));
    }
    return cursor;
  }

  public CFCursor queryCql(String cql) {
    return new CFCursor(this,new ColumnFamilyResultWrapper(ByteBufferSerializer.get(), ByteBufferSerializer.get(),
            keyspace.doExecuteOperation(new CqlOperation(this, cql))));
  }

  class CqlOperation extends Operation<Map<ByteBuffer,List<ColumnOrSuperColumn>>> {

    private ColumnFamily columFamily;
    private String cql;

    CqlOperation(ColumnFamily columFamily, String cql) {
      super(OperationType.READ);
      this.columFamily = columFamily;
      this.cql = cql;
    }

    @Override
    public Map<ByteBuffer,List<ColumnOrSuperColumn>> execute(Cassandra.Client cassandra) throws HectorException {
      try {
        CqlResult result = cassandra.execute_cql_query(StringSerializer.get().toByteBuffer(cql), Compression.NONE);
        switch (result.getType()) {
          case VOID:
            return null;
          default:
            if ( result.getRowsSize() > 0 ) {
              LinkedHashMap<ByteBuffer, List<ColumnOrSuperColumn>> ret = new LinkedHashMap<ByteBuffer, List<ColumnOrSuperColumn>>(result.getRowsSize());
              for (Iterator<CqlRow> rowsIter = result.getRowsIterator(); rowsIter.hasNext(); ) {
                CqlRow row = rowsIter.next();
                ret.put(ByteBuffer.wrap(row.getKey()), filterKeyColumn(row));
              }
              return ret;
            }
            break;
        }
      } catch (Exception ex) {
        throw keyspace.getExceptionsTranslator().translate(ex);
      }
      return null;
    }


    /*
    * Trims the first column from the row if it's name is equal to "KEY",
    * convert underlying list to ColumnOrSuperColumn since CQL only deals with columns
    */
    private List<ColumnOrSuperColumn> filterKeyColumn(CqlRow row) {
      List<ColumnOrSuperColumn> converted = new ArrayList<ColumnOrSuperColumn>();
      if ( row.isSetColumns() && row.columns.size() > 0) {
        Iterator<Column> columnsIterator = row.getColumnsIterator();
        Column column = columnsIterator.next();
        if ( column.name.duplicate().equals(KEY_BB) ) {
          columnsIterator.remove();
        } else {
          converted.add(new ColumnOrSuperColumn().setColumn(column));
        }
        while (columnsIterator.hasNext()) {
          converted.add(new ColumnOrSuperColumn().setColumn(columnsIterator.next()));
        }
      }
      return converted;
    }

    private  ByteBuffer KEY_BB = StringSerializer.get().toByteBuffer("KEY");
  }

}
