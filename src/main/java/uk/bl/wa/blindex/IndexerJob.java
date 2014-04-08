package uk.bl.wa.blindex;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVParser;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class IndexerJob {
	private static final Logger LOG = LoggerFactory.getLogger(IndexerJob.class);

	protected static String solrHomeZipName = "solrHome.zip";

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
			Mapper<LongWritable, Text, IntWritable, SolrInputDocument> {

		private CSVParser p = new CSVParser();
		private Solate sp;
		private String domidUrlPrefix;


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
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, SolrInputDocument> output,
				Reporter reporter)
				throws IOException {

			// String[] parts = value.toString().split("\\x01");
			String[] parts = p.parseLine(value.toString());

			// If this is the header line, return now:
			if ("entityid".equals(parts[0]))
				return;

			// Otherwise, grab the content info:
			String entityuid = parts[1];
			String simpletitle = parts[3];
			String originalname = parts[5];
			String domid = parts[8];

			// Construct URL:
			URL xmlUrl = new URL(domidUrlPrefix + domid);

			// Pass to the SAX-based parser to collect the outputs:
			List<String> docs = null;
			try {
				docs = JISC2TextExtractor.extract(xmlUrl.openStream());
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}

			for (int i = 0; i < docs.size(); i++) {
				// Skip empty records:
				if (docs.get(i).length() == 0)
					continue;
				// Build up a Solr document:
				String doc_id = entityuid + "/p" + i;
				SolrInputDocument doc = new SolrInputDocument();
				doc.setField("id", doc_id);
				doc.setField("simpletitle_s", simpletitle);
				doc.setField("originalname_s", originalname);
				doc.setField("domid_i", domid);
				doc.setField("page_i", i);
				doc.setField("content", docs.get(i));
				output.collect(new IntWritable(sp.getPartition(doc_id, doc)),
						doc);
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
			Reducer<IntWritable, SolrInputDocument, Text, IntWritable> {

		private FileSystem fs;
		private Path solrHomeDir;
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
			super.configure(job);
			try {
				// Filesystem:
				fs = FileSystem.get(job);
				// Input:
				solrHomeDir = findSolrConfig(job, solrHomeZipName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// Output:
			outputDir = new Path("/user/admin/jisc2/solr/");
		}


		public void reduce(IntWritable key, Iterator<SolrInputDocument> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int slice = key.get();

			Path outputShardDir = new Path(outputDir, this.shardPrefix + slice);

			EmbeddedSolrServer solrServer = JISC2TextExtractor
					.createEmbeddedSolrServer(solrHomeDir, fs, outputShardDir);

			while (values.hasNext()) {
				SolrInputDocument doc = values.next();
				try {
					solrServer.add(doc);
				} catch (SolrServerException e) {
					e.printStackTrace();
				}

			}
			output.collect(new Text("" + key), new IntWritable(1));
		}

	}

	public static Path findSolrConfig(JobConf conf, String zipName)
			throws IOException {
	    Path solrHome = null;
		Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
	    if (localArchives.length == 0) {
			throw new IOException(String.format("No local cache archives."));
	    }
	    for (Path unpackedDir : localArchives) {
	      if (unpackedDir.getName().equals(zipName)) {
	        LOG.info("Using this unpacked directory as solr home: {}", unpackedDir);
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

		JobConf conf = new JobConf(IndexerJob.Map.class);
		conf.setJobName("JISC2_Indexer");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setNumMapTasks(4);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(4);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// /user/admin/jisc2-xmls/table_sample.csv
		FileInputFormat.setInputPaths(conf, new Path(
				"/user/admin/jisc2-xmls/table_sample.csv"));
		// /user/admin/jist2/solr
		FileOutputFormat.setOutputPath(conf, new Path("/user/admin/jisc2/job"));

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