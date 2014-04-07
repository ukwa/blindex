package uk.bl.wa.blindex;

import java.io.FileInputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.Solate;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class JISC2TextExtractor {

	private static final class SaxHandler extends DefaultHandler {
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
	}

	public static List<String> extract(InputStream is) throws Exception {

		// creates and returns new instance of SAX-implementation:
		SAXParserFactory factory = SAXParserFactory.newInstance();

		// create SAX-parser...
		SAXParser parser = factory.newSAXParser();

		// .. define our handler:
		SaxHandler handler = new SaxHandler();

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
