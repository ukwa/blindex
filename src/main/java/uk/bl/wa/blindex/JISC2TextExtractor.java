package uk.bl.wa.blindex;

import java.io.FileInputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.hadoop.Solate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class JISC2TextExtractor extends DefaultHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(JISC2TextExtractor.class);
		private List<String> pages = new ArrayList<String>();
		private Writer w;
		private FilterWriter fw;
		private boolean isInText = false;
	private int currentPage = 0;

		// invoked when document-parsing is started:
		public void startDocument() throws SAXException {
		}

		// notifies about finish of parsing:
		public void endDocument() throws SAXException {
		}

		public void startElement(String uri, String localName, String qName,
				Attributes attrs) {
			if ("page".equals(qName)) {
				currentPage++;
				this.w = new StringWriter();
				this.fw = new SpaceTrimWriter(w);
			}
			if ("text".equals(qName)) {
				isInText = true;
			}
		}

		public void characters(char[] ch, int start, int length) {
			if (this.isInText) {
				try {
					fw.write(ch, start, length);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		public void endElement(String uri, String localName, String qName) {
			// End text element:
			if ("text".equals(qName)) {
				isInText = false;
			}
			// End page element:
			if ("page".equals(qName)) {
				pages.add(this.w.toString());
			}
		}

		public List<String> getOutputTest() {
			// return this.textBuilder.toString().trim().replace("\n", " ");
			return this.pages;
		}

	public static List<String> extract(InputStream is) throws Exception {

		// creates and returns new instance of SAX-implementation:
		SAXParserFactory factory = SAXParserFactory.newInstance();

		// create SAX-parser...
		SAXParser parser = factory.newSAXParser();

		// .. define our handler:
		JISC2TextExtractor handler = new JISC2TextExtractor();

		// and parse:
		parser.parse(is, handler);

		//
		return handler.getOutputTest();
	}

	public static void main(String[] args) throws Exception {
		String zkHost = "openstack2.ad.bl.uk:2181,openstack4.ad.bl.uk:2181,openstack5.ad.bl.uk:2181/solr";
		String collection = "jisc2";
		int numShards = 4;

		Solate sp = new Solate(zkHost, collection, numShards);

		CloudSolrServer solrServer = new CloudSolrServer(zkHost);
		solrServer.setDefaultCollection(collection);

		// Extract:
		List<String> txt = extract(new FileInputStream(
				"src/test/resources/562949954724281.xml"));

		// Print
		String item_id = "lsidyv10a49";
		System.out.println("Pages: " + txt.size());
		List<SolrInputDocument> sid = new ArrayList<SolrInputDocument>();
		for (int i = 0; i < txt.size(); i++) {
			// Skip empty records:
			if (txt.get(i).length() == 0)
				continue;
			// Build up a Solr document:
			String doc_id = item_id + "/p" + i;
			SolrInputDocument doc = new SolrInputDocument();
			doc.setField("id", doc_id);
			doc.setField("page_i", i);
			doc.setField("content", txt.get(i));
			System.out.println("shard: " + sp.getPartition(doc_id, doc));
			sid.add(doc);
		}
		solrServer.add(sid);
		solrServer.commit();
	}

	/* --- */

	public static EmbeddedSolrServer createEmbeddedSolrServer(Path solrHomeDir,
			FileSystem fs, Path outputDir, Path outputShardDir)
			throws IOException {

		if (solrHomeDir == null) {
			throw new IOException("Unable to find solr home setting");
		}
		LOG.info("Creating embedded Solr server with solrHomeDir: "
				+ solrHomeDir + ", fs: " + fs + ", outputShardDir: "
				+ outputShardDir);

		Properties props = new Properties();
		// FIXME note this is odd (no scheme) given Solr doesn't currently
		// support uris (just abs/relative path)
		Path solrDataDir = new Path(outputShardDir, "data");
		if (!fs.exists(solrDataDir) && !fs.mkdirs(solrDataDir)) {
			throw new IOException("Unable to create " + solrDataDir);
		}

		String dataDirStr = solrDataDir.toUri().toString();
		LOG.info("Attempting to set data dir to:" + dataDirStr);
		props.setProperty("solr.data.dir", dataDirStr);
		props.setProperty("solr.home", solrHomeDir.toString());
		props.setProperty("solr.solr.home", solrHomeDir.toString());
		props.setProperty("solr.hdfs.home", outputDir.toString());

		SolrResourceLoader loader = new SolrResourceLoader(
				solrHomeDir.toString(), null, props);

		LOG.info(String
				.format("Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
						solrHomeDir, solrHomeDir.toUri(),
						loader.getInstanceDir(), loader.getConfigDir(),
						dataDirStr, outputShardDir));

		CoreContainer container = new CoreContainer(loader);
		container.load();
		LOG.error("Setting up core1 descriptor...");
		CoreDescriptor descr = new CoreDescriptor(container, "core1", new Path(
				solrHomeDir, "jisc2").toString());

		descr.setDataDir(dataDirStr);
		descr.setCoreProperties(props);
		LOG.error("Creating core1... " + descr.getConfigName());
		SolrCore core = container.create(descr);
		LOG.error("Registering core1...");
		container.register(core, false);

		System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
		System.setProperty("solr.hdfs.blockcache.enabled", "false");
		System.setProperty("solr.autoCommit.maxTime", "-1");
		System.setProperty("solr.autoSoftCommit.maxTime", "-1");
		EmbeddedSolrServer solr = new EmbeddedSolrServer(container, "core1");
		return solr;
	}
}

/*
 * <dynamicField name="*_i" type="int" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_is" type="int" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_s" type="string" indexed="true" stored="true" />
 * 
 * <dynamicField name="*_ss" type="string" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_l" type="long" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_ls" type="long" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_t" type="text_general" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_txt" type="text_general" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_en" type="text_en" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_b" type="boolean" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_bs" type="boolean" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_f" type="float" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_fs" type="float" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_d" type="double" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_ds" type="double" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <!-- Type used to index the lat and lon components for the "location"
 * FieldType -->
 * 
 * <dynamicField name="*_coordinate" type="tdouble" indexed="true"
 * stored="false" />
 * 
 * <dynamicField name="*_dt" type="date" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_dts" type="date" indexed="true" stored="true"
 * multiValued="true"/>
 * 
 * <dynamicField name="*_p" type="location" indexed="true" stored="true"/>
 * 
 * <!-- some trie-coded dynamic fields for faster range queries -->
 * 
 * <dynamicField name="*_ti" type="tint" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_tl" type="tlong" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_tf" type="tfloat" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_td" type="tdouble" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_tdt" type="tdate" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_pi" type="pint" indexed="true" stored="true"/>
 * 
 * <dynamicField name="*_c" type="currency" indexed="true" stored="true"/>
 * 
 * <dynamicField name="ignored_*" type="ignored" multiValued="true"/>
 * 
 * <dynamicField name="attr_*" type="text_general" indexed="true" stored="true"
 * multiValued="true"/>
 */
