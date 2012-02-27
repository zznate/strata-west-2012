package org.apigee.tutorial;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.BasicUsageExample"
 * @author zznate
 */
public class BasicUsageExample extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(BasicUsageExample.class);

  public static void main(String[] args) {
    init();
    BasicUsageExample basicUsageExample = new BasicUsageExample();
    basicUsageExample.doExecute();
  }

  private void doExecute() {
    Cassandra cassandra = new Cassandra(tutorialCluster);

    Keyspace keyspace = cassandra.getKeyspace(SchemaUtils.TUTORIAL_KEYSPACE_NAME);

    ColumnFamily columnFamily = keyspace.getColumnFamily("BasicUsageExample");

    Row row = new Row();
    row.put("column1", "value1");
    row.put(2, "value2");
    row.put(3, false);
    UUID id = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    row.put(42, id);
    row.put(id, "uuid col name");
    row.increment("counter1",4).increment(1234L,1).increment("counter1",1);

    columnFamily.insert(row);

    // this only uses the 'key' from the row
    CFCursor cursor = columnFamily.query(row);
    Row foundRow = cursor.next();
    log.info("Row key: {}", foundRow.getKey());
    log.info(" column1: {}", foundRow.getString("column1"));
    log.info(" '2': {}", foundRow.getString(2));
    //log.info(" '3': {}", foundRow.getBoolean(3));
    log.info(" UUID: {}", foundRow.getUUID(42));
    log.info(" 'counter1': {}", foundRow.getCount("counter1"));
    log.info(" '1234L': {}", foundRow.getCount(1234L));

  }
}
