package org.apigee.tutorial;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Show the effects of deleting a column and a whole row from some
 * example data. This class also displays two common issues with
 * Cassandra:
 * - Resurrection: insert a new column into a deleted row, "resurrecting" the deleted row
 * - Timestamp mis-match: insert a column with an older timestamp, column wont apply
 *
 * #CLI output for list of columns
 * list TombstoneDemo;
 *
 * #CQL for all rows:
 * select * from TombstoneDemo;
 *
 * #CQL for single row:
 * select * from TombstoneDemo where KEY = key_1;
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.TombstoneDemoQuery"
 * @author zznate
 */
public class TombstoneDemoQuery extends TutorialBase {

  private static Logger log = LoggerFactory.getLogger(TombstoneDemoQuery.class);

  private static StringSerializer stringSerializer = StringSerializer.get();

  public static void main(String[] args) {
    init();
    verifySchema();

    Cassandra cassandra = new Cassandra(tutorialCluster);

    Keyspace keyspace = cassandra.getKeyspace(SchemaUtils.TUTORIAL_KEYSPACE_NAME);

    ColumnFamily columnFamily = keyspace.getColumnFamily(TombstoneDemoInserter.CF_TOMBSTONE_DEMO);

    Row row = new Row().setKey("key_1");
    CFCursor result = columnFamily.query(row);

    printRow(result.next());

    columnFamily.delete(row);

    result = columnFamily.query(row);

    printRow(result.next());

    row.put("column1","some other value");
    columnFamily.insert(row);

    result = columnFamily.query(row);

    printRow(result.next());

  }

  private static void printRow(Row row) {
    log.info("+--------- Row: {} ---------------", row.getKey());
    if ( row.hasColumns() ) {
      log.info("| column1 = {}",row.getString("column1"));
      log.info("| column2 = {}",row.getString("column2"));
    } else {
      log.info("| TOMBSTONED ");
    }
    log.info("+-----------------------------------");
  }

  protected static void verifySchema() {
    if (!schemaUtils.cfExists(TombstoneDemoInserter.CF_TOMBSTONE_DEMO)) {
      throw new TutorialUsageException("You must first run TombstoneDemoInserter before using this tutorial");
    }
  }
}
