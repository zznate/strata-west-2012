package org.apigee.tutorial;

import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;

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

  private void doExecute() {
    // TODO Row#remove(Object columnName);
    // TODO ColumnFamily#remove(Row row);
    // row 1: unmodified
    // row 2: delete columns
    // row 3: delete row
  }



  protected static void verifySchema() {
    if (!schemaUtils.cfExists(TombstoneDemoInserter.CF_TOMBSTONE_DEMO)) {
      throw new TutorialUsageException("You must first run TombstoneDemoInserter before using this tutorial");
    }
  }
}
