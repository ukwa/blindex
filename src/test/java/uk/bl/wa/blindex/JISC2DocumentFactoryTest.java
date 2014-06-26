/**
 * 
 */
package uk.bl.wa.blindex;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class JISC2DocumentFactoryTest {

	private JISC2DocumentFactory jdf;

	private String prefix = "src/test/resources/newspapers-jisc2";

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		File j2 = new File(prefix);
		jdf = new JISC2DocumentFactory(j2.toURI().toString(), ".xml");
	}

	@Test
	public void testCreate() {
		String line = "\"1484\",\"lsidyv10a49\",\"NULL\",\"York Herald\",\"10\",\"YOHD-1877-04-07.xml\",\"7423129\",\"2010-08-27 09:56:59.94\",\"562949954724281\"";
		List<SolrInputDocument> docs = jdf.create(line);
		assertEquals("Error when extracting pages.", 13, docs.size());
	}

	@Test
	public void testViaFile() throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(prefix
				+ "/table_sample_1.csv"));
		//
		int totalRecords = 0;
		//
		while (in.ready()) {
			String value = in.readLine();

			// Pull in the xml and make the Solr documents:
			List<SolrInputDocument> docs = jdf.create(value.toString());
			if (docs != null)
				totalRecords += docs.size();
		}
		//
		assertEquals("Error when extracting pages.", 13, totalRecords);
	}

}
