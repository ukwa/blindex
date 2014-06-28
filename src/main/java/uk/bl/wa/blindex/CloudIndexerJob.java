package uk.bl.wa.blindex;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class CloudIndexerJob {
	private static final Logger LOG = LoggerFactory.getLogger(CloudIndexerJob.class);

	/**
	 * 
	 * This mapper parses the input table, downloads the relevant XML, parses
	 * the content into Solr documents, computes the target SolrCloud slice and
	 * passes them down to the reducer.
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 * 
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, String, IntWritable> {

		private String domidUrlPrefix;
		private JISC2DocumentFactory docFactory;
		private SolrServer solrServer;


		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop
		 * .mapred.JobConf)
		 */
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			//
			String zkHost = job.get("solr.zookeepers"); // "openstack2.ad.bl.uk:2181,openstack4.ad.bl.uk:2181,openstack5.ad.bl.uk:2181/solr";
			String collection = job.get("solr.collection");// "jisc2";
			String solrServerUri = job.get("solr.httpServer");
			//
			domidUrlPrefix = job.get("dls.prefix");
			docFactory = new JISC2DocumentFactory(domidUrlPrefix, "");

			// Set up Solr connection:
			solrServer = new CloudSolrServer(zkHost);
			((CloudSolrServer) solrServer).setDefaultCollection(collection);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<String, IntWritable> output,
				Reporter reporter)
				throws IOException {

			// Pull in the xml and make the Solr documents:
			List<SolrNewspaperDocument> docs = docFactory.create(value
					.toString());

			// Send them to the SolrCloud
			try {
				for (SolrInputDocument doc : docs) {
					solrServer.add(doc);
				}
				output.collect("GOOD", new IntWritable(docs.size()));
			} catch (SolrServerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				output.collect("FAILED", new IntWritable(docs.size()));
			}

		}

	}

	public class IntSumReducer extends MapReduceBase implements
			Reducer<String, IntWritable, String, IntWritable> {

		@Override
		public void reduce(String key, Iterator<IntWritable> values,
				OutputCollector<String, IntWritable> context, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			context.collect(key, new IntWritable(sum));
		}
	}


	/**
	 * c.f. SolrRecordWriter, SolrOutputFormat
	 * 
	 * Cloudera Search defaults to: /solr/jisc2/core_node1 ...but note no
	 * replicas, which is why the shard-to-core mapping looks easy.
	 * 
	 * Take /user/admin/jisc2-xmls/000000_0 Read line-by-line Split on 0x01.
	 * 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(CloudIndexerJob.Map.class);
		conf.setJobName("JISC2_CloudIndexer");
		conf.setMapOutputKeyClass(String.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setNumMapTasks(4);
		conf.setReducerClass(IntSumReducer.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// Get input and output folder from CLARGS:
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setSpeculativeExecution(false);

		JobClient.runJob(conf);

	}
}