package uk.bl.wa.blindex;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

public class SpaceTrimWriter extends FilterWriter {
	private boolean isStartSpace = true;
	private boolean lastCharWasSpace;
	private boolean includedNewline = false;

	public SpaceTrimWriter(Writer out) {
		super(out);
	}

	public void write(char[] cbuf, int off, int len) throws IOException {
		for (int i = off; i < len + off; i++)
			write(cbuf[i]);
	}

	public void write(String str, int off, int len) throws IOException {
		for (int i = off; i < len + off; i++)
			write(str.charAt(i));
	}

	public void write(int c) throws IOException {
		if (c == ' ' || c == '\n' || c == '\t') {
			lastCharWasSpace = true;
			if (c == '\n')
				includedNewline = true;
		} else {
			if (lastCharWasSpace) {
				if (!isStartSpace) {
					if (includedNewline) {
						out.write(' ');
					} else {
						out.write(' ');
					}
				}
				lastCharWasSpace = false;
				includedNewline = false;
			}
			isStartSpace = false;
			out.write(c);
		}
	}
}