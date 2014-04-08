package uk.bl.wa.blindex;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

public class EmbeddedTest {

	private EmbeddedSolrServer server;

	// Before
	public void setUp() throws Exception {

		// Note that the following property could be set through JVM level
		// arguments too
		System.setProperty("solr.solr.home", "src/main/resources/solr");
		System.setProperty("solr.data.dir", "target/solr-test-home");
		CoreContainer coreContainer = new CoreContainer();
		coreContainer.load();
		server = new EmbeddedSolrServer(coreContainer, "");
	}

	// Test
	public void testDelete() throws SolrServerException, IOException {
		// Remove any items from previous executions:
		server.deleteByQuery("*:*");
	}

}
