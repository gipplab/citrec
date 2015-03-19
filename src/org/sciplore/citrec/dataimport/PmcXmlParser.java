/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Mario Lipinski <lipinski@sciplore.org>
    Copyright (C) 2015 Norman Meuschke <meuschke@sciplore.org>

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

package org.sciplore.citrec.dataimport;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.sciplore.citrec.resources.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import spiaotools.SentParDetector;

/**
 * Thread that calls the Handler for XML parsing.
 * @author Norman Meuschke <a href="mailto:meuschke@sciplore.org">meuschke@sciplore.org</a>
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class PmcXmlParser extends Thread {	
	//IO
	private File xmlDir;
	private File txtDir;
	private File dtdDir;
	private IndexWriter indexWriter;

	//SAX Parser
	private SAXParserFactory factory;
	private SAXParser parser;
	private PmcXmlHandler handler;
	private Pattern sentWordSplit; 
	private Pattern multCitSplit;
	private Pattern cleanRefKeys;

	//DB
	private Connection con;
	private PreparedStatement stmtDoc;
	private PreparedStatement stmtAuth;
	private PreparedStatement stmtCit;
	private PreparedStatement stmtRef;
	
	private File f;

	//Utilities
	private static Logger logger = LoggerFactory.getLogger(PmcXmlParser.class);
	public static HashSet<String> validTypes = new HashSet<String>();

	/**
	 * Initialize the XML parser.
	 * 
	 * @param f the XML file
	 * @param db the database connection
	 * @param dtdDir the directory with required DTDs
	 */
	public PmcXmlParser (File f, Connection db, IndexWriter iw, File xmlDir, File txtDir, File dtdDir) {
		this.xmlDir = xmlDir;
		this.txtDir = txtDir;
		this.dtdDir = dtdDir;
		this.con = db;
		this.f = f;
		this.indexWriter = iw;
		validTypes.add("research-article");
		validTypes.add("review-article");
		validTypes.add("case-report");
		validTypes.add("other");
		validTypes.add("brief-report");
		validTypes.add("report");
	}

	/**
	 * Runs the XML parser.
	 */
	public void run() {
		try {
			//Read complete file
			byte fisBuf[] = new byte[(int) f.length()];
			int read = 0;
			FileInputStream fis = new FileInputStream(f);
			while (read < fisBuf.length) {
				read += fis.read(fisBuf, read, fisBuf.length - read);
			}
			
			//Separate body of XML file
			String splitCont[] = new String(fisBuf).split("\\<body.*?\\>|\\</body.*?\\>");
			//Some files in the dataset are scanned and do not offer XML markup for relevant content
			//therefore only files with body tags are processed
			
			if (splitCont.length == 3) {
				String filename[] = f.getAbsolutePath().split(xmlDir.getAbsolutePath());
				if (filename.length != 2 && filename[1].length() < 1) {
					fis.close();
					throw new Exception("Cannot construct filename for txt file.");
				}
				File txtFile = new File(txtDir, filename[1].replaceFirst(".nxml$", ".txt"));
				
				txtFile.getParentFile().mkdirs();
				
				PrintStream fous = new PrintStream(txtFile);

				//Substitute section and paragraph tags
				Pattern searchTerm = Pattern.compile("\\<sec[^\\>]*?\\>|\\<p[^\\>]*?\\>|" +
						"\\</sec[^\\>]*?\\>|\\</p[^\\>]*?\\>");
				Matcher matcher = searchTerm.matcher(splitCont[1]);
				
				//Clean body from other tags
				StringBuffer sectionCleaned = new StringBuffer();

				while (matcher.find()) {
					matcher.appendReplacement(sectionCleaned, "\n");
				}
				matcher.appendTail(sectionCleaned);
				matcher=null;
				
				//Substitute other tags and character entity tags
				searchTerm = Pattern.compile("\\<.*?\\>|\\</.*?\\>|&#\\w{1,6};");
				StringBuffer contentCleaned = new StringBuffer();
				matcher = searchTerm.matcher(sectionCleaned);

				while (matcher.find()) {
					matcher.appendReplacement(contentCleaned, "");
				}
				matcher.appendTail(contentCleaned);
				matcher=null;
				
				fous.append(contentCleaned);
				
				// Split Words and Sentences
				String xml = splitCont[0] + "<body>" + splitWordSentences(splitCont[1]) + "</body>" + splitCont[2];
				
				//Initiate SAX XML parser 
				//			System.clearProperty("javax.xml.parsers.SAXParserFactory");
				//			System.setProperty("javax.xml.parsers.SAXParserFactory","org.apache.xerces.jaxp.SAXParserFactoryImpl") ;
				factory = SAXParserFactory.newInstance();
				factory.setValidating(false);
				factory.setNamespaceAware(false);
				factory.setFeature("http://xml.org/sax/features/validation", false);
				parser = factory.newSAXParser();
	
				//DB update statements
				stmtDoc = con.prepareStatement("INSERT INTO document " +
						"(`pmcId`, `pmId`, `title`, `type`, `year`, `month`, `file`) " +
						"VALUES(?, ?, ?, ?, ?, ?, ?)");
	
				stmtAuth = con.prepareStatement("INSERT INTO author " +
						"(`pmcId`, `lastname`, `firstname`) VALUES(?, ?, ?)");
	
				stmtCit = con.prepareStatement("INSERT INTO citation " +
						"(`document`, `reference`, `cnt`, `citgrp`, `character`, `word`, `sentence`, `paragraph`, `section`) " +
						"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
	
				stmtRef = con.prepareStatement("INSERT INTO reference " +
						"(`document`, `refId`, `refPmid`, `refPmcId`, `refMedId`, `refDoi`, `refAuthKey`, `refTitKey`) " +
						"VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
	
				//Precompiled patterns to be used in content handler
				sentWordSplit = Pattern.compile("\\*�[WS]/�");
				multCitSplit = Pattern.compile("^[\\D&&\\W]?[\\s]*?(\\d+)[\\s]*?[\\D&&\\W]?[\\s]*?[\u2010\u2011\u2012\\u2013\u2212\\u002D\u00AD][\\s]*?[\\D&&\\W]?[\\s]*?(\\d+)[\\s]*?[\\D&&\\W]?$");
				cleanRefKeys = Pattern.compile("[^\\p{Lower}\\p{Upper}]"); 

				Document doc = new Document();
				//Parse file
				try {
					doc.file = f.getAbsolutePath().replaceFirst(xmlDir.getAbsolutePath() + File.separator, "");
					handler = new PmcXmlHandler(f, stmtDoc, stmtAuth, stmtCit, stmtRef, multCitSplit, sentWordSplit, cleanRefKeys, dtdDir.getAbsolutePath(), doc);
					parser.parse(new InputSource(new StringReader(xml)), handler);
				} 
				//Errors during parsing or moving files
				catch (Exception eParse) {
					logger.error("Error: " + eParse.getMessage() + ": "+f);
					eParse.printStackTrace();
				}
				finally {
					try {
						stmtDoc.close();
						stmtAuth.close();
						stmtCit.close();
						stmtRef.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					handler = null;
				} //Clear document specific data structures	
				
				if (validTypes.contains(doc.type)) {
					org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
					luceneDoc.add(new NumericField("pmcId", Store.YES, false).setIntValue(doc.pmcId));
					luceneDoc.add(new Field("file", filename[1], Store.YES, Index.NO));
					luceneDoc.add(new Field("title", doc.title, Store.YES, Index.ANALYZED, TermVector.YES));
					luceneDoc.add(new Field("abstract", doc.abstractText.toString(), Store.YES, Index.ANALYZED, TermVector.YES));
					luceneDoc.add(new Field("text", contentCleaned.toString(), Store.YES, Index.ANALYZED, TermVector.YES));
					indexWriter.addDocument(luceneDoc);
					luceneDoc = null;
				}

				doc = null;
				contentCleaned = null;
				filename = null;
				txtFile = null;
				fous.close();
				searchTerm = null;
				matcher = null;
				xml = null;
				factory = null;
				parser = null;
				
			} else { // no body
				logger.warn("{} has no body ot too many. Skipping.", f.getAbsolutePath());
			}
			splitCont = null;
			fis.close();;
			fisBuf = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private StringBuffer splitWordSentences(String xml) {
		//Data structures for storing replaced XML markups and cleaned content
		HashMap <Integer, String> xmlContRepl = new HashMap <Integer, String>();

		//Regular expression for preliminary XML removal 
		Pattern xmlRangeRemovalP = Pattern.compile(
				//Replacing content of citations, formulas, tables, figures, chemical structures -> are ignored for word split-up						
				"[ ]?\\<xref(?:\\s)*ref-type=\"bibr\".*?\\>.*?\\</xref\\>|"+
				"[ ]?\\<disp-formula.*?\\>.*?\\</disp-formula\\>|"+
				"[ ]?\\<inline-formula.*?\\>.*?\\</inline-formula\\>|"+
				"[ ]?\\<table(?!-).*?\\>.*?\\</table\\>|"+
				"[ ]?\\<tex-math.*?\\>.*?\\</tex-math\\>|"+
				"[ ]?\\<mml:math.*?\\>.*?\\</mml:math\\>|"+
				"[ ]?\\<chem-struct(?!-).*?\\>.*?\\</chem-struct\\>", Pattern.DOTALL);

		Pattern xmlTagRemovalP = Pattern.compile(
				//Replacing tags only, but not encapsulated content for all other tags and character entities
				"(?:[ ]?\\<.*?\\>)|(?:[ ]?&#\\w{1,6};)", Pattern.DOTALL);

		//Regular expression for word split-up         
		Pattern wordSplitterP = Pattern.compile(				
				//1. pattern (words separated by whitespace(s)) are matched by looking for:
				//last alphanumeric char. in a sequence (word or numeric expression), not being part of a XML substitution        		
				"(?:(?:[a-zA-Z0-9](?!\\w|/�|([\\.,]?[0-9]+?)))" +
				//optionally followed by all non-alphanumeric char. except white spaces or alphanumeric char. that are part of XML substitution
				"(?:[^\\w\\s]|(?:[S0-9]+(?=/�)))*"+
				//mandatorily followed by a white space char
				"(?:\\s)+"+
				//optionally followed by all non-alphanumeric char. except whitespace(s) + alphanumeric char. that are part of XML substitution
				"(?:[^\\w]|(?:S/�)|(?:[0-9]+(?=/�)))*"+
				//mandatorily followed by an alphanumeric char. not being part of a XML substitution 
				"(?:[a-zA-Z0-9](?!/�)))"+
				//OR
				"|"+
				//2. pattern (words separated by XMl replacement, but no whitespace(s)) are matched by looking for:
				//last alphanumeric char. in a sequence (word or numeric expression), self not being part of a XML substitution, 
				//but being directly followed by a XML substitution 
				"(?:(?:[a-zA-Z0-9](?!(?:\\w)|(?:/�)|(?:[\\.,]?[0-9]+?))[,\\.;\\?!\"'\\=/:&+\\-\\$%�]*(?=\\*�[0-9]+/�))" + 
				//followed by arbitrary characters 
				"(?:[^\\w]|(?:S/�)|(?:[0-9]+(?=/�)))*"+
				//until finding an alphanumeric char. not being part of a XML substitution
				"(?:[a-zA-Z0-9](?!(?:/�))))",Pattern.DOTALL);

		//Regex for reinserting XML markup
		Pattern xmlReIns = Pattern.compile("\\*�[0-9]+/�");

		//Preliminary XML range replacement (increases accuracy of sentence splitter)
		Matcher matcher = xmlRangeRemovalP.matcher(xml);
		StringBuffer rangesRem = new StringBuffer();
		int replID=0;
		while (matcher.find()) {
			matcher.appendReplacement(rangesRem, " Z\\*�"+replID+"/�");
			xmlContRepl.put(new Integer(replID),matcher.group());
			replID++;

		}
		matcher.appendTail(rangesRem);
		matcher=null;
		
		//Preliminary XML tag (none-range) replacement (increases accuracy of sentence splitter)
		matcher = xmlTagRemovalP.matcher(rangesRem);
		StringBuffer cleanedContent = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(cleanedContent, " Z\\*�"+replID+"/�");
			xmlContRepl.put(new Integer(replID),matcher.group());
			replID++;

		}
		matcher.appendTail(cleanedContent);
		matcher=null;
		rangesRem = null;
		
		//Sentence parsing
		SentParDetector sentDetect = new SentParDetector();
		String sentMarked = sentDetect.markupRawText(4,cleanedContent.toString());						
		cleanedContent=null;

		//Replace XML markup of sentence parser with own one for consistency during later on processing
		sentMarked = sentMarked.replaceAll("<s n=\"\\d+\">Z(\\*�[0-9]+/�)","\\*�S\\/�$1");
		sentMarked = sentMarked.replaceAll(" Z(\\*�[0-9]+/�)","$1");
		sentMarked = sentMarked.replaceAll("\\<s.*?\\>","\\*�S/�");
		sentMarked = sentMarked.replaceAll("\\</s\\>","");

		//Word markup 
		matcher = wordSplitterP.matcher(sentMarked);
		StringBuffer wordsMarked = new StringBuffer();
		int apStart=0;
		while (matcher.find(apStart)) {
			wordsMarked.append(sentMarked.substring(apStart, matcher.start()));
			if(matcher.group().contains("; ")) {
				wordsMarked.append(matcher.group().replaceFirst(" ", "*�W/�"));
			} else {
				wordsMarked.append(matcher.group().charAt(0)+"*�W/�"+matcher.group().substring(1,matcher.group().length()-1));
			}
			apStart=matcher.end()-1;
		}
		wordsMarked.append(sentMarked.substring(apStart, sentMarked.length()));
		sentMarked=null;
		matcher=null;

		//XML Re-substitution
		matcher = xmlReIns.matcher(wordsMarked);
		StringBuffer finalMarkup = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(finalMarkup, Matcher.quoteReplacement(xmlContRepl.get(Integer.parseInt(matcher.group().substring(2,matcher.group().length()-2)))));
		}

		// Return results
		return finalMarkup;
	}
}
