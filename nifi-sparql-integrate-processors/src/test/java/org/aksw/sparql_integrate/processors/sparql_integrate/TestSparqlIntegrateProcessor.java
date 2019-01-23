package org.aksw.sparql_integrate.processors.sparql_integrate;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

// import org.apache.nifi.util.TestRunner;
// import org.apache.nifi.util.TestRunners;
// import org.junit.Before;

public class TestSparqlIntegrateProcessor {

  // @Test
  // public void testQueryInFlowFile() throws IOException {
  //   final TestRunner runner = TestRunners.newTestRunner(new SparqlIntegrateProcessor());
  //   runner.enqueue(Paths.get("src/test/ressources/TestSparqlIntegrateProcessor/test-query.sparql"));
  //   runner.run();
  //   runner.assertTransferCount(SparqlIntegrateProcessor.SUCCESS, 1);
  //   MockFlowFile successFile =
  //       runner.getFlowFilesForRelationship(SparqlIntegrateProcessor.SUCCESS).get(0);
  //   successFile.assertContentEquals(
  //       Paths.get("src/test/ressources/TestSparqlIntegrateProcessor/test-dataset.nq"));
  // }

  // private TestRunner testRunner;

  // @Before
  // public void init() {
  // testRunner = TestRunners.newTestRunner(SparqlIntegrateProcessor.class);
  // }

  // @Test
  // public void testProcessor() {

  // }

}
