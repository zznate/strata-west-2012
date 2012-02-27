package org.apigee.tutorial;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class Keyspace {

  private final ExecutingKeyspace keyspace;  
  private final ThriftCluster thriftCluster;
  private final String keyspaceName;
  
  Keyspace(String keyspaceName, ThriftCluster cluster) {
    this.keyspaceName = keyspaceName;
    this.thriftCluster = cluster;
    keyspace = new ExecutingKeyspace(keyspaceName, thriftCluster.getConnectionManager(),
        HFactory.createDefaultConsistencyLevelPolicy(), FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);  
  }
  
  public List<ColumnFamily> getColumnFamilies() {    
    KeyspaceDefinition ksDef = thriftCluster.describeKeyspace(keyspaceName);
    List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
    for (ColumnFamilyDefinition cfd  : ksDef.getCfDefs()) {      
      columnFamilies.add(new ColumnFamily(cfd.getName(), keyspace));
    }
    return columnFamilies;
  }
  
  public ColumnFamily getColumnFamily(String columnFamilyName) {
    ColumnFamily columnFamily = new ColumnFamily(columnFamilyName, keyspace);
    List<ColumnFamilyDefinition> cfDefs = thriftCluster.describeKeyspace(keyspaceName).getCfDefs();
    boolean found = false;
    for ( ColumnFamilyDefinition cfDef : cfDefs ) {
      if (cfDef.getName().equals(columnFamilyName) ) {
        found = true;
        break;
      }
    }
    if ( !found ) {
      if (!Cassandra.COUNTER_CF_NAME.equals(columnFamilyName) ) {
        ThriftCfDef cfDef = new ThriftCfDef(keyspaceName, columnFamilyName);
        thriftCluster.addColumnFamily(cfDef);
      } else {
        thriftCluster.addColumnFamily(Cassandra.buildStockCfs(keyspaceName));
      }
    }

    return columnFamily;
  }

  public String getName() {
    return keyspaceName;
  }

  /**
   * Simplistic Equals - compares on {@link #getName}
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Keyspace))
      return false;
    return ((Keyspace)o).getName().equals(keyspaceName);
  }
}
