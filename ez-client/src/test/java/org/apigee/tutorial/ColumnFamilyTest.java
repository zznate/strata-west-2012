package org.apigee.tutorial;

import static org.junit.Assert.*;

import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.apigee.tutorial.CFCursor;
import org.apigee.tutorial.ColumnFamily;
import org.apigee.tutorial.Keyspace;
import org.apigee.tutorial.Row;
import org.junit.Test;

public class ColumnFamilyTest extends BaseCassandraTest {
  
  @Test
  public void insertion() {
    Keyspace keyspace = cassandra.getKeyspace("ColumnFamilyTest");
    ColumnFamily columnFamily = keyspace.getColumnFamily("Insertion");
    Row row = new Row();
    row.put("column1", "value1");
    row.put(2, "value2");
    row.put(3, false);
    UUID id = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    row.put(42, id);
    row.put(id, "uuid col name");
    row.increment("counter1",4).increment(1234L,1).increment("counter1",1);


    columnFamily.insert(row);   

    CFCursor cursor = columnFamily.query(row);
    Row foundRow = cursor.next();
    assertEquals(row.getKey(), foundRow.getKey());
    assertEquals("value1",foundRow.getString("column1"));
    assertEquals("value2",foundRow.getString(2));
    assertEquals(false, foundRow.getBoolean(3));
    assertEquals(id,foundRow.getUUID(42));
    assertEquals("uuid col name", foundRow.getString(id));
    assertNull(foundRow.getString("didnt store this"));
    assertEquals(5,foundRow.getCount("counter1"));
    assertEquals(1,foundRow.getCount(1234L));
  }
}
