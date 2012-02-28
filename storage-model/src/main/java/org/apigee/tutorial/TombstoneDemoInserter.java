package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Creates a few static rows of data modelling users. The interesting part takes place in
 * TombstoneDemoQuery.
 *
 * #CQL
 * To do a batch insert via CQL, use the following syntax:
 *
 * #NOTE
 * Inserting timeseries data is one of the places where CQL gets a bit cumbersome.
 *
 * @author zznate
 */
public class TombstoneDemoInserter extends TutorialBase {

  private Logger log = LoggerFactory.getLogger(TombstoneDemoInserter.class);

  public static final String CF_TOMBSTONE_DEMO = "TombstoneDemo";

  public static void main(String[] args) {
    init();
    maybeCreateSchema();

    Cassandra cassandra = new Cassandra(tutorialCluster);

    Keyspace keyspace = cassandra.getKeyspace(SchemaUtils.TUTORIAL_KEYSPACE_NAME);

    ColumnFamily columnFamily = keyspace.getColumnFamily(CF_TOMBSTONE_DEMO);

    Row row;
    for(int x=0; x<10; x++) {
      row = new Row();
      row.setKey("key"+x);
      row.put("k"+x+"_column1", "value1");
      row.put("k"+x+"_column2","value2");
      columnFamily.insert(row);
    }
  }

  protected static void maybeCreateSchema() {
    BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
    columnFamilyDefinition.setKeyspaceName(SchemaUtils.TUTORIAL_KEYSPACE_NAME);
    columnFamilyDefinition.setName(CF_TOMBSTONE_DEMO);
    columnFamilyDefinition.setComparatorType(ComparatorType.UTF8TYPE);
    columnFamilyDefinition.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());
    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
    schemaUtils.maybeCreate(cfDef);
  }


}
