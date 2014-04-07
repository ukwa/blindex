package blindex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class JISC2TextExtractor {

	private static final class SaxHandler extends DefaultHandler {
		private StringBuilder textBuilder = new StringBuilder();
		Writer w = new StringWriter();
		FilterWriter fw = new SpaceTrimWriter(w);
		private boolean isInText = false;

		// invoked when document-parsing is started:
		public void startDocument() throws SAXException {
		}

		// notifies about finish of parsing:
		public void endDocument() throws SAXException {
		}

		public void startElement(String uri, String localName, String qName,
				Attributes attrs) {
			// textBuilder = new StringBuilder();
			if ("text".equals(qName)) {
				isInText = true;
			}
		}

		public void characters(char[] ch, int start, int length) {
			if (this.isInText) {
			textBuilder.append(ch, start, length);
			try {
				fw.write(ch, start, length);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}

		public void endElement(String uri, String localName, String qName) {
			// String theCompleteText = this.textBuilder.toString();
			// System.out.println("TEXT: "
			// + theCompleteText.trim().replace("\n", " "));
			if ("text".equals(qName)) {
				isInText = false;
			}
		}

		public String getOutputTest() {
			// return this.textBuilder.toString().trim().replace("\n", " ");
			return this.w.toString();
		}
	}

	public static String extract(InputStream is) throws Exception {

		// creates and returns new instance of SAX-implementation:
		SAXParserFactory factory = SAXParserFactory.newInstance();

		// create SAX-parser...
		SAXParser parser = factory.newSAXParser();

		// .. define our handler:
		SaxHandler handler = new SaxHandler();

		// and parse:
		parser.parse(new File("562949954724281.xml"), handler);

		//
		return handler.getOutputTest();
	}

	public static void main(String[] args) throws Exception {
		// Extract:
		String txt = extract(new FileInputStream("562949954724281.xml"));

		// Print
		System.out.println(txt);
	}
}