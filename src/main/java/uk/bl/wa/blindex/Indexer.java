package uk.bl.wa.blindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class Indexer {
	private static final Logger LOG = LoggerFactory.getLogger(Indexer.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		File input = new File(args[0]); // "list.csv"
		String solrServerUri = args[1]; // "http://localhost:8xxx/solr/collection1"
		String domidUrlPrefix = args[2]; // "http://194.66.239.142/did/";
		String suffix = "";

		long lineCounter = 0;

		if (args.length >= 4) {
			suffix = args[3];
		}

		// Set up the document factory:
		JISC2DocumentFactory docFactory = new JISC2DocumentFactory(
				domidUrlPrefix, suffix);

		// Set up Solr connection:
		SolrServer solrServer = new HttpSolrServer(solrServerUri);

		// Go through the input file:
		BufferedReader in = new BufferedReader(new FileReader(input));
		while (in.ready()) {
			String value = in.readLine();
			lineCounter++;

			// Pull in the xml and make the Solr documents:
			List<SolrNewspaperDocument> docs = docFactory.create(value
					.toString());

			// Send them to the SolrCloud
			if (docs != null) {
				LOG.info("For line " + lineCounter + " got " + docs.size()
						+ " docs. Title[0] = "
						+ docs.get(0).getFieldValue("article_title_s"));
				List<SolrInputDocument> sindocs = new ArrayList<SolrInputDocument>();
				for (SolrNewspaperDocument doc : docs) {
					sindocs.add(doc);
					LOG.debug("Got doc with title: "
							+ doc.getFieldValue("article_title_s"));
				}
				// And send to Solr:
				try {
					LOG.debug("Sending " + sindocs.size() + " docs to Solr...");
					solrServer.add(sindocs);
				} catch (SolrServerException e) {
					LOG.error(e.getMessage());
					e.printStackTrace();
				}
			}

		}
		// Clean up:
		in.close();
		// Commit:
		solrServer.commit();
	}
}