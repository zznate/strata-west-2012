package org.apigee.tutorial.common;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

import java.io.IOException;
import java.util.Properties;

public abstract class TutorialBase {
  protected static Cluster tutorialCluster;
  protected static Keyspace tutorialKeyspace;
  protected static Properties properties;
  protected static SchemaUtils schemaUtils;

  protected static void init(boolean skipProps) {
    properties = new Properties();
    if ( !skipProps) {

      try {
        properties.load(TutorialBase.class.getResourceAsStream("/tutorial.properties"));
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    // To modify the default ConsistencyLevel of QUORUM, create a
    // me.prettyprint.hector.api.ConsistencyLevelPolicy and use the overloaded form:
    // tutorialKeyspace = HFactory.createKeyspace("Tutorial", tutorialCluster, consistencyLevelPolicy);
    // see also me.prettyprint.tutorial.model.ConfigurableConsistencyLevelPolicy[Test] for details
    try {
      tutorialCluster = HFactory.getOrCreateCluster(properties.getProperty("cluster.name", "TutorialCluster"),
              properties.getProperty("cluster.hosts", "127.0.0.1:9160"));
      schemaUtils = new SchemaUtils(tutorialCluster);
      schemaUtils.maybeCreateKeyspace();
      ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
      ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
      tutorialKeyspace = HFactory.createKeyspace(SchemaUtils.TUTORIAL_KEYSPACE_NAME, tutorialCluster, ccl);

    } catch(HectorException he) {
      he.printStackTrace();
    }

  }



  protected static void init() {
    init(true);
  }

}
