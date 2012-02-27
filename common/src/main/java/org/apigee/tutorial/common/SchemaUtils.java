package org.apigee.tutorial.common;

import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

/**
 * Common utility class for checking schema existence and
 * creating if necessary.
 *
 * @author zznate
 */
public class SchemaUtils {

  public static String TUTORIAL_KEYSPACE_NAME = "TutorialKeyspace";

  private Cluster cluster;

  public SchemaUtils(Cluster cluster) {
    this.cluster = cluster;
  }

  public boolean cfExists(String columnFamilyNme) {
    return false;
  }

  public void maybeCreate(ColumnFamilyDefinition cfDef) {
    if ( cfExists(cfDef.getName() ) ) {
      return;
    }
  }

  /**
   * Mark all existing data in this column family as deleted. Functions
   * similarly to an RDBMS.
   *
   * @param columnFamilyName
   */
  public void truncate(String columnFamilyName) {
    cluster.truncate(TUTORIAL_KEYSPACE_NAME, columnFamilyName);
  }
}
