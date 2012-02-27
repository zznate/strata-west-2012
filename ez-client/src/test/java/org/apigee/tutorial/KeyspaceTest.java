package org.apigee.tutorial;

import static org.junit.Assert.*;

import java.util.List;

import org.apigee.tutorial.ColumnFamily;
import org.apigee.tutorial.Keyspace;
import org.junit.Test;

public class KeyspaceTest extends BaseCassandraTest {
  
  @Test
  public void listColumnFamiliesEmptyOnNew() {
    Keyspace keyspace = cassandra.getKeyspace("EzClientKeyspace");
    List<ColumnFamily> columnFamilies = keyspace.getColumnFamilies();
    assertEquals(1,columnFamilies.size());
    ColumnFamily columFamily = keyspace.getColumnFamily("ezk");
    columnFamilies = keyspace.getColumnFamilies();
    assertEquals(2,columnFamilies.size());
  }


}
