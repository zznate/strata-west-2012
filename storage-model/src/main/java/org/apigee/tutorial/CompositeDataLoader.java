package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Multi-threaded bulk loader for geo data located in the data directory of the
 * project root.
 *
 * This class inserts the columns for a single index row over within ALL.
 * This approach could be used when you had a finite number of static columns where
 * you needed a flexible searching mechanism (via slices). See CompositeQuery for
 * more details on query granularity.
 *
 * Execute this class by invoking the following at the project root:
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.CompositeDataLoader"
 * @author zznate
 *
 */
public class CompositeDataLoader extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(CompositeDataLoader.class);

  private static ExecutorService exec;
  // key for static composite, First row of dynamic composite
  public static final String COMPOSITE_KEY = "ALL";
  public static final String CF_COMPOSITE_INDEX = "CompositeSingleRowIndex";

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    init();
    maybeCreateSchema();
    String fileLocation = properties.getProperty("composites.geodata.file.location","data/geodata.txt");
    BufferedReader reader;
    exec = Executors.newFixedThreadPool(5);
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation)));
      // read 1000 and hand off to worker

      List<String> lines = new ArrayList<String>(1000);
      String line = reader.readLine();

      List<Future<Integer>> sums = new ArrayList<Future<Integer>>();
      while(line != null) {

        lines.add(line);
        if ( lines.size() % 250 == 0 ) {
          doParse(lines, sums);
        }
        line = reader.readLine();
      }
      doParse(lines, sums);

      int total = 0;
      for (Future<Integer> future : sums) {
        // naive wait for completion
        total = total + future.get().intValue();
      }

      log.info("Inserted a total of {} over duration ms: {}", total, System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error("Could not locate file",e);
    } finally {
      exec.shutdown();
    }
    tutorialCluster.getConnectionManager().shutdown();
  }


  protected static void maybeCreateSchema() {
    BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
    columnFamilyDefinition.setKeyspaceName(SchemaUtils.TUTORIAL_KEYSPACE_NAME);
    columnFamilyDefinition.setName(CF_COMPOSITE_INDEX);
    columnFamilyDefinition.setComparatorType(ComparatorType.COMPOSITETYPE);
    columnFamilyDefinition.setComparatorTypeAlias("(UTF8Type, UTF8Type, UTF8Type)");
    columnFamilyDefinition.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());
    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
    schemaUtils.maybeCreate(cfDef);
  }

  private static void doParse(List<String> lines, List<Future<Integer>> sums) {
    Future<Integer> f = exec.submit(new CompositeDataLoader().new LineParser(new ArrayList(lines)));
    sums.add(f);
    lines.clear();
  }


  class LineParser implements Callable<Integer> {

    List<String> lines;
    LineParser(List<String> lines) {
      this.lines = lines;
    }

    public Integer call() throws Exception {
      int count = 0;
      GeoDataLine geoDataLine;
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());

      for (String row : lines) {
        // parse
        geoDataLine = new GeoDataLine(row);
        // assemble the insertions
        mutator.addInsertion(COMPOSITE_KEY, CF_COMPOSITE_INDEX, geoDataLine.staticColumnFrom());
        
        count++;
      }
      mutator.execute();
      log.debug("Inserted {} columns", count);
      return Integer.valueOf(count);
    }

  }

  /**
   * This is probably overkill given the simplicity of the data, but is
   * good practice for separation of concerns and encapsulation
   */
  static class GeoDataLine {
    public static final char SEPARATOR_CHAR = ',';
    private String[] vals = new String[10];

    GeoDataLine(String line) {
      vals = StringUtils.split(StringEscapeUtils.unescapeCsv(line), SEPARATOR_CHAR);
      log.debug("array size: {} for row: {}", vals.length, line);
    }

    /**
     * Creates an HColumn with a column name composite of the form:
     *   ['country_code']:['state]:['city name'])
     * and a value of ['timezone']
     * @return
     */
    HColumn<Composite,String> staticColumnFrom() {

      Composite composite = new Composite();
      composite.addComponent(getCountryCode(), StringSerializer.get());
      composite.addComponent(getAdmin1Code(), StringSerializer.get());
      // extra un-escape to handle the case of "Washington, D.C." 
      composite.addComponent(StringEscapeUtils.unescapeCsv(getAsciiName()), StringSerializer.get());
      HColumn<Composite,String> col =
        HFactory.createColumn(composite, getTimezone(), new CompositeSerializer(), StringSerializer.get());
      return col;
    }


    String getCountryCode() {
      return vals[0];
    }
    String getAdmin1Code() {
      return vals[1];
    }
    String getAsciiName() {
      return vals[2];
    }
    String getTimezone() {
      return vals[3];
    }
  }
}
