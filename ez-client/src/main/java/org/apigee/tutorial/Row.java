package org.apigee.tutorial;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Models an row in an Apache Cassandra column family. Allows for 
 * generic storage and retrieval of basic types while maintaining 
 * type safety. Supports counters by deffering to an automatically
 * created column family, storing the counter columns under the same key
 *  
 * @author zznate
 */
public class Row {
  
  private String key;
  private Map<ByteBuffer,HColumn<ByteBuffer, ByteBuffer>> columnMap =
    new HashMap<ByteBuffer,HColumn<ByteBuffer, ByteBuffer>>();
  private Map<ByteBuffer,HCounterColumn<ByteBuffer>> counterMap =
      new HashMap<ByteBuffer,HCounterColumn<ByteBuffer>>();

  private static MicrosecondsSyncClockResolution clockResolution = new MicrosecondsSyncClockResolution();
  private static final TypeInferringSerializer tis = TypeInferringSerializer.get();
  private ColumnFamily columnFamily;

  public Row() {
  }

  Row apply(ColumnFamily columnFamily) {
    this.columnFamily = columnFamily;
    return this;
  }

  public Row setKey(String key) {
    this.key = key;
    return this;
  }

  public Row increment(Object columnName, long value) {
    // add this to the 'application countrs' CF under this key
    ByteBuffer colName = tis.toByteBuffer(columnName);
    HCounterColumn<ByteBuffer> col = counterMap.get(colName);
    if ( col == null ) {
      col = new HCounterColumnImpl<ByteBuffer>(colName, value, ByteBufferSerializer.get());
    } else {
      col.setValue(col.getValue() + value);
    }
    counterMap.put(colName,col);
    return this;
  }
   
  public Row put(Object columnName, Object columnValue) {
    ByteBuffer colName = tis.toByteBuffer(columnName);
    columnMap.put(colName,
            new HColumnImpl<ByteBuffer, ByteBuffer>(colName,
                    columnValue == null ? ByteBuffer.wrap(new byte[0]) : tis.toByteBuffer(columnValue),
                    clockResolution.createClock(),
                    ByteBufferSerializer.get(), ByteBufferSerializer.get()));
    return this;
  }

  public boolean hasCounters() {
    return this.counterMap.size() > 0;
  }

  public long getCount(Object columnName) {
    // execute a CQL counter query independent of the colmap
    String cql = "Select '%s' from %s where KEY = %s";
    CFCursor cursor = columnFamily.queryCql(String.format(cql, ByteBufferUtil.bytesToHex(tis.toByteBuffer(columnName)),
            Cassandra.COUNTER_CF_NAME, this.key));
    return cursor.next().get(LongSerializer.get(), tis.toByteBuffer(columnName));
  }
    
  public boolean hasColumns() {
    return !columnMap.isEmpty();
  }
   
  public String getString(Object columnName) {
    return get(StringSerializer.get(), columnName);
  }
  
  public int getInt(Object columnName) {
    return get(IntegerSerializer.get(), columnName);
  }
  
  public long getLong(Object columnName) {
    return get(LongSerializer.get(), columnName);
  }
  
  public Date getDate(Object columnName) {
    return get(DateSerializer.get(), columnName);
  }
  
  public UUID getUUID(Object columnName) {
    return get(UUIDSerializer.get(), columnName);
  }
  
  public boolean getBoolean(Object columnName) {
    return get(BooleanSerializer.get(), columnName);
  }
  
  public byte[] getBytes(Object columnName) {
    return get(BytesArraySerializer.get(), columnName);
  }
  
  /**
   * Generic method that preserves typing. Use this method with custom 
   * value serializers
   */
  public <T> T get(Serializer<T> serializer, Object columnName) {
    HColumn<ByteBuffer, ByteBuffer> column = columnMap.get(tis.toByteBuffer(columnName));
    if ( column == null ) {
      return null;
    }
    return serializer.fromByteBuffer(column.getValue());
  }
  
  ByteBuffer getKeyBytes() {
    return StringSerializer.get().toByteBuffer(getKey());
  }

  Map<ByteBuffer,HColumn<ByteBuffer, ByteBuffer>> getColumns() {
    return columnMap;
  }

  Map<ByteBuffer,HCounterColumn<ByteBuffer>> getCounters() {
    return counterMap;
  }

  List<ByteBuffer> getColumnsForQuery() {
    List<ByteBuffer> cols = new ArrayList<ByteBuffer>(columnMap.size());
    for (ByteBuffer buf : columnMap.keySet()) {
      cols.add(columnMap.get(buf).getName());
    }
    return cols;
  }
  
  String getKey() {
    if ( key == null )
      key = TimeUUIDUtils.getTimeUUID(clockResolution).toString();
    return key;
  }
  
  void put(ByteBuffer columnName, HColumn<ByteBuffer,ByteBuffer> column) {
    columnMap.put(columnName, column);
  }
}
