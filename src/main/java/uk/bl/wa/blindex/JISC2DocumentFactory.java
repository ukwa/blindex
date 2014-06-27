/**
 * 
 */
package uk.bl.wa.blindex;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import au.com.bytecode.opencsv.CSVParser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class JISC2DocumentFactory {

	private CSVParser p = new CSVParser();

	private String dlsPrefix = null;
	private String urlSuffix = null;

	public JISC2DocumentFactory(String dlsPrefix, String suffix) {
		this.dlsPrefix = dlsPrefix;
		this.urlSuffix = suffix;
	}

	/**
	 * Create a set of SolrDocuments based on a line.
	 * 
	 * @param line
	 */
	public List<SolrNewspaperDocument> create(String line) {
		// String[] parts = value.toString().split("\\x01");
		String[] parts;
		try {
			parts = p.parseLine(line.toString());
		} catch (IOException e2) {
			e2.printStackTrace();
			return null;
		}

		// If this is the header line, return now:
		if ("entityid".equals(parts[0])) {
			return null;
		}

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
			xmlUrl = new URL(this.dlsPrefix + "/" + domid + this.urlSuffix);
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
			return null;
		}

		// Pass to the SAX-based parser to collect the outputs:
		List<SolrNewspaperDocument> docs = null;
		try {
			docs = JISC2TextExtractor.extract(xmlUrl.openStream());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		for (int i = 0; i < docs.size(); i++) {
			// Get the document:
			SolrNewspaperDocument doc = docs.get(i);

			// Build up a Solr document:
			doc.setField("newspaper_title_s", simpletitle);
			doc.setField("newspaper_internal_id_s", npid);
			doc.setField("issue_original_filename_s", originalname);
			doc.setField("issue_domid_l", domid);
			doc.setField("issue_pub_date_tdt", pubdate);
			doc.setField("issue_digi_date_tdt", recordcreated_dt);
			doc.setField("issue_year_s", year);
		}
		return docs;
	}

}
