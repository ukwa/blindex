package uk.bl.wa.blindex;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.Solate;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class EmbeddedIndexerJob {
	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedIndexerJob.class);

	protected static String solrHomeZipName = "solr_home.zip";

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
			Mapper<LongWritable, Text, IntWritable, SolrInputDocumentWritable> {

		private Solate sp;
		private String domidUrlPrefix;
		private JISC2DocumentFactory docFactory;


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
			String zkHost = "openstack2.ad.bl.uk:2181,openstack4.ad.bl.uk:2181,openstack5.ad.bl.uk:2181/solr";
			String collection = "jisc2";
			int numShards = 4;
			sp = new Solate(zkHost, collection, numShards);
			//
			domidUrlPrefix = "http://194.66.239.142/did/";
			docFactory = new JISC2DocumentFactory(domidUrlPrefix);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, SolrInputDocumentWritable> output,
				Reporter reporter)
				throws IOException {

			// Pull in the xml and make the Solr documents:
			List<SolrInputDocument> docs = docFactory.create(value.toString());

			// Group them by shard number:
			for (SolrInputDocument doc : docs) {
				String doc_id = (String) doc.getFieldValue("id");
				output.collect(new IntWritable(sp.getPartition(doc_id, doc)),
						new SolrInputDocumentWritable(doc));
			}

		}

	}

	/**
	 * 
	 * This reducer collects the documents for each slice together and commits
	 * them to an embedded instance of the Solr server stored on HDFS
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 * 
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<IntWritable, SolrInputDocumentWritable, Text, IntWritable> {

		private FileSystem fs;
		private Path solrHomeDir = null;
		private Path outputDir;
		private String shardPrefix = "shard";

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop
		 * .mapred.JobConf)
		 */
		@Override
		public void configure(JobConf job) {
			LOG.info("Calling configure()...");
			super.configure(job);
			try {
				// Filesystem:
				fs = FileSystem.get(job);
				// Input:
				solrHomeDir = findSolrConfig(job, solrHomeZipName);
				LOG.info("Found solrHomeDir " + solrHomeDir);
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error("FAILED in reducer configuration: " + e);
			}
			// Output:
			outputDir = new Path("/user/admin/jisc2/solr/");
		}


		public void reduce(IntWritable key,
				Iterator<SolrInputDocumentWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int slice = key.get();
			int countTotal = 0;
			int countFailures = 0;

			Path outputShardDir = new Path(outputDir, this.shardPrefix + slice);

			LOG.info("Running reducer for " + slice + " > " + outputShardDir);

			//
			EmbeddedSolrServer solrServer = JISC2TextExtractor
					.createEmbeddedSolrServer(solrHomeDir, fs, outputDir,
							outputShardDir);

			while (values.hasNext()) {
				SolrInputDocument doc = values.next().getSolrInputDocument();
				countTotal++;
				try {
					solrServer.add(doc);
					output.collect(new Text("" + key), new IntWritable(1));
				} catch (Exception e) {
					LOG.error("ERROR " + e + " when adding document "
							+ doc.getFieldValue("id"));
					countFailures++;
				}
			}

			try {
				solrServer.commit();
				solrServer.shutdown();
			} catch (SolrServerException e) {
				LOG.error("ERROR on commit: " + e);
			}
			output.collect(new Text("TOTAL " + key),
					new IntWritable(countTotal));
			output.collect(new Text("FAILURES " + key), new IntWritable(
					countFailures));
		}

	}

	public static Path findSolrConfig(JobConf conf, String zipName)
			throws IOException {
	    Path solrHome = null;
		Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
	    if (localArchives.length == 0) {
			LOG.error("No local cache archives.");
			throw new IOException(String.format("No local cache archives."));
	    }
	    for (Path unpackedDir : localArchives) {
			LOG.info("Looking at: " + unpackedDir + " for " + zipName);
			if (unpackedDir.getName().equals(zipName)) {
				LOG.info("Using this unpacked directory as solr home: {}",
						unpackedDir);
				solrHome = unpackedDir;
				break;
			}
	    }
	    return solrHome;
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

		JobConf conf = new JobConf(EmbeddedIndexerJob.Map.class);
		conf.setJobName("JISC2_Indexer");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(SolrInputDocumentWritable.class);
		conf.setMapperClass(Map.class);
		conf.setNumMapTasks(4);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(4);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// Get input and output folder from CLARGS:
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		conf.setSpeculativeExecution(false);

		// File solrHomeZip = new
		// File("src/main/resources/jisc2/solr_home.zip");

		Path zipPath = new Path("/user/admin/jisc2-xmls/solr_home.zip");
		FileSystem fs = FileSystem.get(conf);
		// fs.copyFromLocalFile(new Path(solrHomeZip.toString()), zipPath);
		final URI baseZipUrl = fs.getUri().resolve(
				zipPath.toString() + '#' + solrHomeZipName);

		DistributedCache.addCacheArchive(baseZipUrl, conf);
		LOG.debug("Set Solr distributed cache: {}",
				Arrays.asList(DistributedCache.getCacheArchives(conf)));
		LOG.debug("Set zipPath: {}", zipPath);

		JobClient.runJob(conf);

	}
}