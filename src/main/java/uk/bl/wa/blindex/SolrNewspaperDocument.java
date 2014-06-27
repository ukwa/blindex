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
		this.setField("content_length_ti", text.length());
	}

	public String getText() {
		return (String) this.getFieldValue("content");
	}

	public int getTextLength() {
		if (this.getFieldValue("content_length_ti") == null)
			return 0;
		return (Integer) this.getFieldValue("content_length_ti");
	}

	/**
	 * 
	 * @param page
	 */
	public void setPage(int page) {
		setField("page_i", page);
	}

	public int getPage() {
		return (Integer) this.getFieldValue("page_i");
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