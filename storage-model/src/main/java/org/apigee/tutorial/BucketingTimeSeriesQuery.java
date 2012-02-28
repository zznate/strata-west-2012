package org.apigee.tutorial;

import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;

/**
 *
 * mvn -e exec:java -Dexec.args='[key]' -Dexec.mainClass="org.apigee.tutorial.BucketingTimeSeriesQuery"
 * @author zznate
 */
public class BucketingTimeSeriesQuery extends TutorialBase {

  public static void main(String[] args) {
    init();
    checkSchema();
  }

  protected static void checkSchema() {
    if (!schemaUtils.cfExists(BucketingTimeSeriesInserter.CF_BUCKETED_TIMSERIES)) {
      throw new TutorialUsageException("You must first run BucketingTimeSeriesInserter before using this tutorial.");
    }
  }


}
