package org.apigee.tutorial;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
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
 * TODO
 *
 * #CQL for insert:
 * TODO
 *
 * #CQL for all rows:
 * TODO
 *
 * #CQL for single row:
 * TODO
 * @author zznate
 */
public class TombstoneDemoQuery extends TutorialBase {

  private static Logger log = LoggerFactory.getLogger(TombstoneDemoQuery.class);

  private static StringSerializer stringSerializer = StringSerializer.get();

  public static void main(String[] args) {
    init();
    verifySchema();
    RangeSlicesQuery<String, String, String> rangeSlicesQuery =
        HFactory.createRangeSlicesQuery(tutorialKeyspace, stringSerializer, stringSerializer, stringSerializer);
    rangeSlicesQuery.setColumnFamily(TombstoneDemoInserter.CF_TOMBSTONE_DEMO);
    rangeSlicesQuery.setKeys("", "");
    rangeSlicesQuery.setRange("", "", false, 3);
    rangeSlicesQuery.setRowCount(10);

    QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
    OrderedRows<String, String, String> orderedRows = result.get();
    for (me.prettyprint.hector.api.beans.Row<String, String, String> row : orderedRows) {
        int keyNum = Integer.valueOf(row.getKey().substring(9));
        log.info("+-----------------------------------");
        if ( keyNum % 2 == 0 ) {
            log.info("| result key:" + row.getKey() + " which should have values: " + row.getColumnSlice());
        } else {
            log.info("| TOMBSTONED result key:" + row.getKey() + " has values: " + row.getColumnSlice());
        }
        SliceQuery<String, String, String> q =
                HFactory.createSliceQuery(tutorialKeyspace, stringSerializer, stringSerializer, stringSerializer);
        q.setColumnFamily(TombstoneDemoInserter.CF_TOMBSTONE_DEMO);
        q.setRange("", "", false, 3);
        q.setKey(row.getKey());

        QueryResult<ColumnSlice<String, String>> r = q.execute();
        log.info("|-- called directly via get_slice, the value is: " +r);
        // For a tombstone, you just get a null back from ColumnQuery
        log.info("|-- try the first column via getColumn: " + HFactory.createColumnQuery(tutorialKeyspace,
                stringSerializer, stringSerializer, stringSerializer).setColumnFamily("Standard1").setKey(row.getKey()).setName("fake_column_0").execute());

        log.info("|-- verify on CLI with: get Keyspace1.Standard1['" + row.getKey() + "'] ");
    }
  }




  protected static void verifySchema() {
    if (!schemaUtils.cfExists(TombstoneDemoInserter.CF_TOMBSTONE_DEMO)) {
      throw new TutorialUsageException("You must first run TombstoneDemoInserter before using this tutorial");
    }
  }
}
