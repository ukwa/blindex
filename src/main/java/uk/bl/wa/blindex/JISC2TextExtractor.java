package uk.bl.wa.blindex;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class JISC2TextExtractor extends DefaultHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(JISC2TextExtractor.class);
	private List<SolrNewspaperDocument> docs = new ArrayList<SolrNewspaperDocument>();
	private FilterWriter fw;
	private Writer w;
	private StringWriter ew;
	private boolean isInText = false;
	private int currentPage = 0;
	private int currentArticle = 0;
	private String dayOfWeek;
	private String issueId;
	private String newspaperId;

	enum State {
		OUTER, PAGE, ARTICLE, TEXT
	}

	private State state = State.OUTER;

	SolrNewspaperDocument d;


	// invoked when document-parsing is started:
	public void startDocument() throws SAXException {
	}

	// notifies about finish of parsing:
	public void endDocument() throws SAXException {
	}

	public void startElement(String uri, String localName, String qName,
			Attributes attrs) {
		// Set up a new string-builder for any character-only elements:
		ew = new StringWriter();
		// Switch states:
		if ("page".equals(qName)) {
			currentPage++;
			state = State.PAGE;
		} else if ("article".equals(qName)) {
			currentArticle++;
			d = new SolrNewspaperDocument();
			d.setField("issue_day_of_week_s", this.dayOfWeek);
			d.setField("issue_id_s", this.issueId);
			d.setField("newspaper_id_s", this.newspaperId);
			this.w = new StringWriter();
			this.fw = new SpaceTrimWriter(w);
			state = State.ARTICLE;
		} else if ("ocr".equals(qName)) {
			this.d.setField("article_ocr_relevant_s",
					attrs.getValue("relevant"));
		} else if ("text".equals(qName)) {
			state = State.TEXT;
			isInText = true;
		}
	}

	public void characters(char[] ch, int start, int length) {
		try {
			if (this.isInText) {
				fw.write(ch, start, length);
			} else {
				ew.write(ch, start, length);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void endElement(String uri, String localName, String qName) {
		// End text element:
		if ("text".equals(qName)) {
			isInText = false;
			state = State.ARTICLE;

		} else if ("ti".equals(qName) && state == State.ARTICLE) {
			String art_title = ew.toString().trim().replace("\n", " ");
			LOG.debug("Found article title: " + art_title);
			d.setField("article_title_s",art_title);

		} else if ("ct".equals(qName) && state == State.ARTICLE) {
			d.setField("article_type_s",
					ew.toString().trim().replace("\n", " "));

		} else if ("ocr".equals(qName) && state == State.ARTICLE) {
			d.setField("article_ocr_quality_td",
					ew.toString().trim().replace("\n", " "));

		} else if ("id".equals(qName) && state == State.ARTICLE) {
			String art_id = ew.toString().trim().replace("\n", " ");
			d.setField("article_id_s", art_id);
			d.setField("id", "JISC2-" + art_id);
			LOG.debug("Found article id: " + art_id);

		} else if ("article".equals(qName)) {
			// End article
			d.setText(this.w.toString().trim());
			d.setPage(this.currentPage);
			d.setField("page_i", currentPage);
			d.setField("article_i", currentArticle);
			docs.add(d);
			state = State.PAGE;

		} else if ("page".equals(qName)) {
			// End page element:
			currentArticle = 0;
			state = State.OUTER;

		} else if ("dw".equals(qName) && state == state.OUTER) {
			dayOfWeek = ew.toString().trim().replace("\n", " ");

		} else if ("id".equals(qName) && state == state.OUTER) {
			issueId = ew.toString().trim().replace("\n", " ");

		} else if ("newspaperID".equals(qName) && state == state.OUTER) {
			newspaperId = ew.toString().trim().replace("\n", " ");

		}
	}

	public List<SolrNewspaperDocument> getSolrDocuments() {
		// return this.textBuilder.toString().trim().replace("\n", " ");
		return this.docs;
	}

	public static List<SolrNewspaperDocument> extract(InputStream is)
			throws Exception {

		// creates and returns new instance of SAX-implementation:
		SAXParserFactory factory = SAXParserFactory.newInstance();

		// create SAX-parser...
		SAXParser parser = factory.newSAXParser();

		// .. define our handler:
		JISC2TextExtractor handler = new JISC2TextExtractor();

		// and parse:
		parser.parse(is, handler);

		//
		return handler.getSolrDocuments();
	}

	/* --- */
}

