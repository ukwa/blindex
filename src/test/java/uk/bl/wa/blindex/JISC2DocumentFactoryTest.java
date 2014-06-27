/**
 * 
 */
package uk.bl.wa.blindex;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
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

	private static boolean PRINT_SOLR_DOCS = false;

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
		List<SolrNewspaperDocument> docs = jdf.create(line);
		// Printy:
		if (PRINT_SOLR_DOCS) {
			System.out.println("---");
			for (SolrInputDocument doc : docs) {
				prettyPrint(System.out, (SolrInputDocument) doc);
				System.out.println("---");
			}
		}
		// checks:
		assertEquals("Error when extracting solr documents.", 67, docs.size());
		assertEquals("Text length: ", 221, docs.get(0).getTextLength());
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
			List<SolrNewspaperDocument> docs = jdf.create(value
					.toString());
			if (docs != null)
				totalRecords += docs.size();
		}
		in.close();
		//
		assertEquals("Error when extracting pages.", 67, totalRecords);
	}

	/**
	 * Pretty-print each solrDocument in the results to stdout
	 * 
	 * @param out
	 * @param doc
	 */
	private static void prettyPrint(PrintStream out, SolrInputDocument doc) {
		List<String> sortedFieldNames = new ArrayList<String>(
				doc.getFieldNames());
		Collections.sort(sortedFieldNames);
		out.println();
		for (String field : sortedFieldNames) {
			out.println(String.format("%s: %.100s", field,
					doc.getFieldValue(field)));
		}
		out.println();
	}
}
