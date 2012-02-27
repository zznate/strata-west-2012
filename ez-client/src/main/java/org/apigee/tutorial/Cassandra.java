package org.apigee.tutorial;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
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

  public static final String COUNTER_CF_NAME = "EzClientCountersCf";

  /**
   * Automatically connect to a Cassandra instance running the Thrift API
   * on the default port of 9160 bound to the 'localhost' address. This is
   * the default in a stock Cassandra configuration ("rpc_address" and
   * "rpc_port" from cassandra.yaml).
   */
  public Cassandra() {
    this("localhost:9160",false);
  }

  /**
   * Specify a one or more hosts (separated by commas) in the form:
   * [ip address]:[port]
   *
   * For example, for a three node cluster with hosts named cass1,
   * cass2, and cass3 running the thrift API on port 9171 ("rpc_port from
   * cassandra.yaml - the default is 9160), the connection string would be:
   * "cass1:9171,cass2:9171,cass3:9171"
   *
   * @param hosts
   */
  public Cassandra(String hosts) {
    this(hosts,false);
  }

  /**
   * Same as above, but with the ability to specify whether host auto
   * discovery should be on or off.
   *
   * @param hosts
   * @param autoDiscover
   */
  public Cassandra(String hosts, boolean autoDiscover) {
    cassandraHostConfigurator = new CassandraHostConfigurator(hosts);
    cassandraHostConfigurator.setAutoDiscoverHosts(autoDiscover);
    if (autoDiscover)
      cassandraHostConfigurator.setRunAutoDiscoveryAtStartup(true);
    thriftCluster = new ThriftCluster("CassandraCluster", cassandraHostConfigurator);
  }

  /**
   * Build this istance with an existing cluster. Sets the internal
   * {@link CassandraHostConfigurator} to the instance available from
   * the cluster.
   *
   * @param cluster
   */
  public Cassandra(Cluster cluster) {
    this.thriftCluster = (ThriftCluster)cluster;
    cassandraHostConfigurator = thriftCluster.getConfigurator();
  }

  /**
   * Get a Keyspace, creating it if necessary, creating also the counter cf
   * if needed
   * @param keyspaceName
   * @return
   */
  public Keyspace getKeyspace(String keyspaceName) {
    Keyspace keyspace = new Keyspace(keyspaceName, thriftCluster);
    if (! getKeyspaces().contains(keyspace) ) {
      try {
        buildEzClientKeyspace(keyspaceName);
      } catch (HectorException he) {
        he.printStackTrace();
      }
    } if ( keyspace.getColumnFamily(COUNTER_CF_NAME) == null ) {
      thriftCluster.addColumnFamily(buildStockCfs(keyspaceName));
    }
    log.info("Found counter CF"+keyspace.getColumnFamily(COUNTER_CF_NAME));
    return keyspace;
  }

  private void buildEzClientKeyspace(String keyspaceName) {
    log.info("attempting keyspace create");
    thriftCluster.addKeyspace(new ThriftKsDef(keyspaceName, "SimpleStrategy",
            1, Arrays.asList(buildStockCfs(keyspaceName))), true);
    log.info("keyspace created");
  }

  static ColumnFamilyDefinition buildStockCfs(String keyspaceName) {
    ThriftCfDef cfDef = new ThriftCfDef(keyspaceName, COUNTER_CF_NAME);
    cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
    return cfDef;
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
