package uk.bl.wa.blindex;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.Test;

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

	@Test
	public void testSplitter() {
		String originalname = "YOHD-1877-04-07.xml";
		String[] on_parts = originalname.replace(".xml", "").split("-");
		String npid = on_parts[0];
		String year = on_parts[1];
		String month = on_parts[2];
		String day = on_parts[3];
		String pubdate = year+"-"+month+"-"+day;
		
		System.out.println(pubdate + " " + npid);

	}

}
