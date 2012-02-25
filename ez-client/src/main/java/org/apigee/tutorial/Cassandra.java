package org.apigee.tutorial;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Models the basics of a connection to Apache Cassandra
 *
 */
public class Cassandra {
  private Logger log = LoggerFactory.getLogger(Cassandra.class);

  private final CassandraHostConfigurator cassandraHostConfigurator;
  private final ThriftCluster thriftCluster;

  final String COUNTER_CF_NAME = "EzClientCountersCf";
  
  public Cassandra() {
    this("localhost:9161",false);
  }
  
  public Cassandra(String hosts) {
    this(hosts,false);
  }
  
  public Cassandra(String hosts, boolean autoDiscover) {
    cassandraHostConfigurator = new CassandraHostConfigurator(hosts);
    cassandraHostConfigurator.setAutoDiscoverHosts(autoDiscover);
    if (autoDiscover)
      cassandraHostConfigurator.setRunAutoDiscoveryAtStartup(true);
    thriftCluster = new ThriftCluster("CassandraCluster", cassandraHostConfigurator);
  }

  /**
   * Get a Keyspace, creating it if necessary
   * @param keyspaceName
   * @return
   */
  public Keyspace getKeyspace(String keyspaceName) {
    Keyspace keyspace = new Keyspace(keyspaceName, thriftCluster);
    if (! getKeyspaces().contains(keyspace) ) {
      buildEzClientKeyspace(keyspaceName);
    }
    return keyspace;
  }

  private void buildEzClientKeyspace(String keyspaceName) {
    log.info("attempting keyspace create");
    ThriftCfDef cfDef = new ThriftCfDef(keyspaceName, COUNTER_CF_NAME);
    cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.toString());
    List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
    cfDefs.add(cfDef);
    thriftCluster.addKeyspace(new ThriftKsDef(keyspaceName, "SimpleStrategy", 1, cfDefs), true);
  }
  
  public List<Keyspace> getKeyspaces() {
    List<Keyspace> keyspaces = new ArrayList<Keyspace>();
    for( KeyspaceDefinition kd : thriftCluster.describeKeyspaces() ) {      
      if( !kd.getName().equals("system") ) {
        keyspaces.add(new Keyspace(kd.getName(), thriftCluster));
        log.info("found keyspace: {}", kd.getName());
      }
    }  
    return keyspaces;
  }
  
  public boolean connected() {
    return thriftCluster.describeClusterName() != null;
  }
}
