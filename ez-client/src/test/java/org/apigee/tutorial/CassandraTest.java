package org.apigee.tutorial;

import static org.junit.Assert.*;

import java.util.List;

import org.apigee.tutorial.Keyspace;
import org.junit.Before;
import org.junit.Test;


/**
 * test for Cassandra class
 */
public class CassandraTest extends BaseCassandraTest {
    
  
  @Test
  public void basicStartup() {   
    assertTrue(cassandra.connected());
  }
  
  @Test
  public void acquireKeyspace() {
    Keyspace keyspace = cassandra.getKeyspace("Keyspace1");
    assertNotNull(keyspace);
  }
  
  @Test
  public void listKeyspaces() {
    List<Keyspace> keyspaces = cassandra.getKeyspaces();
    assertTrue(keyspaces.size() > 0);
  }

}
