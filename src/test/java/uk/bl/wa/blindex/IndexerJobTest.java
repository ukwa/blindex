package uk.bl.wa.blindex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.junit.Before;
import org.junit.Test;

public class IndexerJobTest {

	MapDriver<LongWritable, Text, IntWritable, SolrInputDocumentWritable> mapDriver;
	ReduceDriver<IntWritable, SolrInputDocumentWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, IntWritable, SolrInputDocumentWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() throws Exception {
		IndexerJob.Map mapper = new IndexerJob.Map();
		IndexerJob.Reduce reducer = new IndexerJob.Reduce();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		//

	    mapDriver.withInput(new LongWritable(), new Text(
	    		"\"entityid\",\"entityuid\",\"parentid\",\"simpletitle\",\"contentstreamid\",\"originalname\",\"sizebytes\",\"recordcreated_dt\",\"domid\""));
		mapDriver.resetOutput();
	    mapDriver.runTest();
	    
		// mapDriver.withInput(new LongWritable(), new Text(
		// "\"1484\",\"lsidyv10a49\",\"NULL\",\"York Herald\",\"10\",\"YOHD-1877-04-07.xml\",\"7423129\",\"2010-08-27 09:56:59.94\",\"562949954724281\""));
		// mapDriver.withAllOutput(outputRecords);
		mapDriver.runTest();
	  }

/*	 

	  @Test
	  public void testReducer() {
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("6"), values);
	    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    reduceDriver.runTest();
	  }
	  
*/
}
