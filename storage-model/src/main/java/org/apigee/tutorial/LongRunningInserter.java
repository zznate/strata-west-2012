package org.apigee.tutorial;

import org.apigee.tutorial.common.TutorialBase;

import java.util.concurrent.Callable;

/**
 * Low-intensity, long-running inserts. Use in combination with command-line
 * or monitoring systems such as DataStax's OpsCenter, Munin, Ganglia or similar.
 *
 * Defaults to 1 thread with a 10 second sleep between 1000 inserts. These parameters
 * are adjustable so you can test thresholds of a simple workload.
 *
 * Various was to see what is happening:
 * Column Family healt and workload statistics via 'nodetool' CLI:
 * nodetool -h localhost cfstats
 *
 * Thread pool usage statistics also via 'nodetool' CLI:
 * nodetool -h localhost tpstats
 *
 * DataStax OpsCenter:
 * TODO add screen instructions
 *
 * @author zznate
 */
public class LongRunningInserter extends TutorialBase {

  public static void main(String[] args ) {
    // executor runs single insert-then-sleep
    // - sleep is a long enough window to exploit flush semantics

  }

  protected void maybeCreateSchema() {
    // TODO
  }

  private void doInsert() {

  }

  class Inserter implements Callable {

    @Override
    public Object call() throws Exception {
      return null;
    }
  }
}
