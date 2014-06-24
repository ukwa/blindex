/**
 * 
 */
package uk.bl.wa.blindex;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import au.com.bytecode.opencsv.CSVParser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class JISC2DocumentFactory {

	private CSVParser p = new CSVParser();

	private String dlsPrefix = null;

	public JISC2DocumentFactory(String dlsPrefix) {
		this.dlsPrefix = dlsPrefix;
	}

	/**
	 * Create a set of SolrDocuments based on a line.
	 * 
	 * @param line
	 */
	public List<SolrInputDocument> create(String line) {
		// String[] parts = value.toString().split("\\x01");
		String[] parts;
		try {
			parts = p.parseLine(line.toString());
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}

		// If this is the header line, return now:
		if ("entityid".equals(parts[0]))
			return null;

		// Otherwise, grab the content info:
		// "entityid","entityuid","parentid","simpletitle","contentstreamid","originalname","sizebytes","recordcreated_dt","domid"
		String entityuid = parts[1];
		String simpletitle = parts[3];
		String originalname = parts[5];
		String recordcreated_dt = parts[7].replaceFirst(" ", "T") + "Z";
		String domid = parts[8];

		// Split up originalname to get parts
		String[] on_parts = originalname.replace(".xml", "").split("-");
		String npid = on_parts[0];
		String year = on_parts[1];
		String month = on_parts[2];
		String day = on_parts[3];
		String pubdate = year + "-" + month + "-" + day + "T00:00:00Z";

		// Construct URL:
		URL xmlUrl;
		try {
			xmlUrl = new URL(this.dlsPrefix + domid);
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
			return null;
		}

		// Pass to the SAX-based parser to collect the outputs:
		List<String> docs = null;
		try {
			docs = JISC2TextExtractor.extract(xmlUrl.openStream());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		List<SolrInputDocument> solrDocs = new ArrayList<SolrInputDocument>();
		for (int i = 0; i < docs.size(); i++) {
			// Skip empty records:
			if (docs.get(i).length() == 0)
				continue;

			// Page number:
			int page = i + 1;

			// Build up a Solr document:
			String doc_id = entityuid + "/p" + page;
			SolrInputDocument doc = new SolrInputDocument();
			doc.setField("id", doc_id);
			doc.setField("simpletitle_s", simpletitle);
			doc.setField("npid_s", npid);
			doc.setField("originalname_s", originalname);
			doc.setField("domid_l", domid);
			doc.setField("page_i", page);
			doc.setField("pubdate_dt", pubdate);
			doc.setField("digidate_dt", recordcreated_dt);
			doc.setField("year_s", year);
			doc.setField("content", docs.get(i));
			solrDocs.add(doc);
		}
		return solrDocs;
	}

}
