package uk.bl.wa.blindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

		// Set up the document factory:
		JISC2DocumentFactory docFactory = new JISC2DocumentFactory(
				domidUrlPrefix, "");

		// Set up Solr connection:
		SolrServer solrServer = new HttpSolrServer(solrServerUri);

		// Go through the input file:
		BufferedReader in = new BufferedReader(new FileReader(input));
		while (in.ready()) {
			String value = in.readLine();

			// Pull in the xml and make the Solr documents:
			List<SolrNewspaperDocument> docs = docFactory.create(value
					.toString());

			// Send them to the SolrCloud
			try {
				for (SolrInputDocument doc : docs) {
					solrServer.add(doc);
				}
			} catch (SolrServerException e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}

		}
		// Clean up:
		in.close();
	}
}