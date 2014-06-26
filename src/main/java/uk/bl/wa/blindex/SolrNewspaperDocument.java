/**
 * 
 */
package uk.bl.wa.blindex;

import org.apache.solr.common.SolrInputDocument;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrNewspaperDocument extends SolrInputDocument {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3736816541263941826L;

	/**
	 * 
	 * @param text
	 */
	public void setText(String text) {
		this.setField("content", text);
		this.setField("content_length_i", text.length());
	}

	/**
	 * 
	 * @return
	 */
	public int getTextLength() {
		if (this.getFieldValue("content_length_i") == null)
			return 0;
		return (Integer) this.getFieldValue("content_length_i");
	}

}
