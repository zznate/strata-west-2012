package org.apigee.tutorial;

import me.prettyprint.hector.testutils.EmbeddedServerHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.transport.TTransportException;
import org.apigee.tutorial.Cassandra;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class BaseCassandraTest {
  private static EmbeddedServerHelper embedded;
  
  protected Cassandra cassandra;
  
  @Before
  public void localSetup() {
    if ( cassandra == null ) {
      cassandra = new Cassandra("localhost:9161");
    }
  }

  /**
   * Set embedded Cassandra up and spawn it in a new thread.
   *
   * @throws TTransportException
   * @throws IOException
   * @throws InterruptedException
   */
  @BeforeClass
  public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
    if ( embedded == null ) {
      embedded = new EmbeddedServerHelper();
      embedded.setup();
    }
  }

}
