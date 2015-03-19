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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sciplore.citrec.resources.Author;
import org.sciplore.citrec.resources.Citation;
import org.sciplore.citrec.resources.Document;
import org.sciplore.citrec.resources.NonMatchingTagsException;
import org.sciplore.citrec.resources.PublicationDate;
import org.sciplore.citrec.resources.Reference;
import org.sciplore.citrec.resources.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.ArrayListMultimap;

/**
 * Handler for parsing PMC XML files.
 * @author Norman Meuschke <a href="mailto:meuschke@sciplore.org">meuschke@sciplore.org</a>
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class PmcXmlHandler extends DefaultHandler {
	private static Logger logger = LoggerFactory.getLogger(PmcXmlHandler.class);
	
	private static final int CIT_GRP_CHAR_OFFSET = 10;
	
	//DB
	private PreparedStatement stmtDoc;
	private PreparedStatement stmtAuth;
	private PreparedStatement stmtCit;
	private PreparedStatement stmtRef;

	//IO
	private File inpFile;
	private String dtdDir;

	//Document
	private Document doc;
	private Author auth;
	private LinkedList <PublicationDate> pubDates = null;
	private int tmpPubYear = 5555;
	private int tmpPubMonth = 99;

	//Sections, paragraphs, sentences, words, charater counts
	private int secLevel;
	private Map<Integer, Integer> secCnt = new HashMap<Integer, Integer>();
	private int pCnt;
	private int sentCnt;
	private int wordCnt;
	private int charCnt;

	//Citations
	private Pattern multCitSplit;
	private Matcher multCitMatch;
	private Citation curCit;
	private Citation prevCit;
	private StringBuilder tmpCitCont;
	private ArrayListMultimap <String,Citation> abbrevCit;
	private int cgCnt;
	private int citCnt;
	private int lastCitCharCnt;

	//References
	private Reference ref;
	private Pattern cleanRefKeyP;
	private Matcher refKeyCleaner;
	private StringBuilder refAuthBuild;
	private ArrayList <String> references;

	//Literal Content
	private StringBuilder curCont;
	private String curTag;
	private Stack<Tag> skipTagContent;
	private Pattern sentWordSplit;
	private Matcher sentWordMatch;

	//Parser conditions
	//General article data
	private boolean isArticleMeta = false;
	private boolean isPmId = false;
	private boolean isPmcId = false;
	private boolean isTitle = false;
	private boolean isRelatedArt = false;
	private boolean isContribGroup = false;
	private boolean isContribAuth = false;
	private boolean isAuthName = false;
	private boolean isPubDate = false;
	private boolean isAbstract = false;

	//Body and sections
	private boolean isBody = false;
	private boolean isFront = false;
	private boolean isSkipElement = false;

	//Citations
	private boolean isCitation = false;
	private boolean considerPrevCit = false;
	private boolean isMultiCit = false;
	private boolean maybeAbbrevCit = false;
	private boolean containsAbbrevCit = false;

	//References	
	private boolean isReflist = false;
	private boolean isReflistItem = false;
	private boolean isRefArtTitle = false;
	private boolean isRefTransTitle = false;
	private boolean isRefTitle = false;
	private boolean isRefSource = false;
	private boolean refArtTitleSet = false;
	private boolean refTransTitleSet = false;
	private boolean refTitleSet = false;
	private boolean isRefPmcId = false;
	private boolean isRefPmId = false;
	private boolean isRefDoi = false;
	private boolean isRefMedID = false;
	private boolean isRefAuthors = false;
	private boolean refAuthorsSet = false;

	/**
	 * Initialize the XML handler.
	 * 
	 * @param inpf File handle for the XML file
	 * @param doc SQL statement for inserting the document data
	 * @param auth SQL statement for inserting the author data
	 * @param cit SQL statement for inserting the citation data
	 * @param ref SQL statement for inserting the reference data
	 * @param mc Regular expression pattern for splitting multiple citations
	 * @param sw Regular expression pattern for splitting sentences and words
	 * @param clRefK Regular expression pattern for cleaning reference keys
	 * @param dtd the directory name with required DTDs
	 */
	public PmcXmlHandler(File inpf, PreparedStatement sdoc, PreparedStatement auth, PreparedStatement cit, 
			PreparedStatement ref, Pattern mc, Pattern sw, Pattern clRefK, String dtd, Document doc) {
		this.inpFile = inpf;
		this.stmtDoc = sdoc;
		this.stmtAuth = auth;
		this.stmtCit = cit;
		this.stmtRef = ref;
		this.multCitSplit = mc;
		this.abbrevCit = ArrayListMultimap.create(30, 2);
		this.sentWordSplit = sw;
		this.cleanRefKeyP = clRefK;
		this.curCont = new StringBuilder();
		this.skipTagContent = new Stack<Tag>();
		this.tmpCitCont = new StringBuilder();
		this.dtdDir = dtd;
		this.doc = doc;
	}

	/**
	 * Handling the start of the document and initialize some stuff.
	 */
	public void startDocument() throws SAXException {
	}

	/**
	 * Handling the end of the document and save abbreviated citations and the 
	 * document
	 */
	public void endDocument () throws SAXException {
		//If abbrev. cit. were detected and stored during parsing the body
		if(containsAbbrevCit) {
			int startIndex, endIndex;
			//For every ref. starting an abbrev. range retrieve the respective cit. ending the range
			for (String startRid : abbrevCit.keySet()) {
				List <Citation> abbrevRanges = abbrevCit.get(startRid);
				//For all cit. ending an abbreviated range
				for(Citation endCit : abbrevRanges) {
					//Match starting and ending ref. to refernece list
					startIndex = references.indexOf(startRid);
					endIndex = references.indexOf(endCit.rid);
					int abbrRangeIter = endIndex - (startIndex+1);
					//Complete the information for citations within the abbreviated range and store them
					for(int i = startIndex+1; i<=endIndex; i++) {
						storeCit(references.get(i), endCit.citCnt-abbrRangeIter, endCit.citGrp, endCit.charCnt, 
								endCit.wordCnt, endCit.sentCnt, endCit.parCnt, endCit.sec);
						abbrRangeIter --;
					}
				}

			}
		}
		if(doc.pmcId != 0 && PmcXmlParser.validTypes.contains(doc.type)) {
			try {
				//pmcId, pmId, title, year, file
				if (doc.title == null) {
					doc.title = "";
				}
				stmtDoc.setInt(1, doc.pmcId);
				stmtDoc.setInt(2, doc.pmId);
				stmtDoc.setString(3, doc.title.trim());
				stmtDoc.setString(4, doc.type);
				stmtDoc.setInt(5, doc.year);
				stmtDoc.setInt(6, doc.month);
				stmtDoc.setString(7, doc.file);
				stmtDoc.executeUpdate();
			} catch (SQLException e) {
				throw new SAXException("ERROR (DB-Update - document): "+e.getMessage());
			}
		}
		// documents without pmcId are not stored
	}


	/**
	 * Handles the start of and XML tag and its arguments.
	 */
	public void startElement (String ns, String ln, String qn, Attributes attr) throws SAXException {
		curTag = qn;
		
		// ------------------------------------- Start tags in body -------------------------------------	
		if(isBody) {
			if(qn.equals("xref")){
				if(attr.getValue("ref-type") != null && attr.getValue("ref-type").equals("bibr")) {
					isCitation = true;
					processCit(attr);
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				else {
					skipTagContent.push(new Tag (qn,false));
					return;
				}
			}
			//Paragraph and section count, except in skipElements and article front section (abstract, article metadata etc.)
			if(qn.equals("p") && !isSkipElement && !isFront && !isReflist) {
				pCnt++;
				skipTagContent.push(new Tag (qn,false));
				return;
			}
			if (qn.equals("sec") && !isFront) {
				if(secCnt.get(++secLevel) == null) {
					secCnt.put(secLevel, 1);
				} 
				else {
					secCnt.put(secLevel, secCnt.get(secLevel)+1);
				}
				skipTagContent.push(new Tag (qn,false));
				return;
			}
			if (qn.equals("table")){skipTagContent.push(new Tag (qn,false));  isSkipElement = true; return;}
			if (qn.equals("disp-formula")){skipTagContent.push(new Tag (qn,false)); isSkipElement = true; return;}
			if (qn.equals("inline-formula")){skipTagContent.push(new Tag (qn,false));  isSkipElement = true; return;}
			if (qn.equals("tex-math")){skipTagContent.push(new Tag (qn,false));  isSkipElement = true; return;}
			if (qn.equals("mml:math")){skipTagContent.push(new Tag (qn,false));  isSkipElement = true; return;}
			if (qn.equals("chem-struct")){skipTagContent.push(new Tag (qn,false));  isSkipElement = true; return;}

			skipTagContent.push(new Tag (qn,false));
			return;
		}// ------------------------------------- /Start tags in body -------------------------------------	

		//Paragraph and section count also throughout the document, except in skipElements and abstract
		if(qn.equals("p") && !isSkipElement && !isFront && !isReflist) {
			pCnt++;
			skipTagContent.push(new Tag (qn,true));
			return;
		}
		if (qn.equals("sec") && !isFront) {
			if(secCnt.get(++secLevel) == null) {
				secCnt.put(secLevel, 1);
			} 
			else {
				secCnt.put(secLevel, secCnt.get(secLevel)+1);
			}
			skipTagContent.push(new Tag (qn,true));
			return;
		}

		// ------------------------------------- Start tags in reference list -------------------------------------	
		if(isReflist){
			if(qn.equals("ref")) {
				isReflistItem = true;
				ref = new Reference(doc.pmcId, attr.getValue("id"));
				skipTagContent.push(new Tag (qn,false));
				return;
			} 
			if(isReflistItem ) {
				if ((qn.equals("person-group"))&& (attr.getValue("person-group-type")!= null) && (attr.getValue("person-group-type").equals("author"))){
					isRefAuthors = true;
					refAuthBuild = new StringBuilder();
					skipTagContent.push(new Tag (qn,false));
					return;	
				} 
				if (isRefAuthors) {
					if(qn.equals("surname")){
						skipTagContent.push(new Tag (qn,false));
						return;
					}
					if (qn.equals("collab")){
						skipTagContent.push(new Tag (qn,false));
						return;
					}
				}
				if (qn.equals("article-title")){
					isRefArtTitle = true;
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				if (qn.equals("title")){
					isRefTitle=true;
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				if (qn.equals("trans-title")){
					isRefTransTitle = true;
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				if (qn.equals("source")){
					isRefSource = true;
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				if(qn.equals("pub-id") || qn.equals("object-id")) {
					if(attr.getValue("pub-id-type").equals("pmc")){
						isRefPmcId = true;
						skipTagContent.push(new Tag (qn,false));
						return;
					} 
					if(attr.getValue("pub-id-type").equals("pmid")) {
						isRefPmId = true;
						skipTagContent.push(new Tag (qn,false));
						return;
					} 
					if(attr.getValue("pub-id-type").equals("doi")) {
						isRefDoi = true;
						skipTagContent.push(new Tag (qn,false));
						return;
					}
					if(attr.getValue("pub-id-type").equals("medline")) {
						isRefMedID = true;
						skipTagContent.push(new Tag (qn,false));
						return;
					}
					skipTagContent.push(new Tag (qn,true));
					return;
				}
				if (qn.equals("collab")){
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				if(!skipTagContent.peek().skip) {
					if(qn.equals("bold") || qn.equals("italic") && qn.equals("underline") || qn.equals("monospace")
							|| qn.equals("named-content") || qn.equals("sub") || qn.equals("sup") || qn.equals("sc")
							|| qn.equals("ext-link") || qn.equals("uri") || qn.equals("xref")){
						skipTagContent.push(new Tag (qn, false));
						return;
					}
				}
				skipTagContent.push(new Tag (qn,true));
				return;
			}
			skipTagContent.push(new Tag (qn,true));
			return;
		}
		// ------------------------------------- /Start tags reference list -------------------------------------	

		// ------------------------------------- Start tags in article metadata -------------------------------------		
		if (isArticleMeta) {
			if(qn.equals("abstract")) {
				isAbstract = true;
				skipTagContent.push(new Tag(qn, false));
				return;
			}

			if(qn.equals("article-id")){
				if(attr.getValue("pub-id-type") != null && attr.getValue("pub-id-type").equals("pmc")) {
					isPmcId = true;
					skipTagContent.push(new Tag (qn,false));
					return;
				} 
				if(attr.getValue("pub-id-type") != null && attr.getValue("pub-id-type").equals("pmid")) {
					isPmId = true;
					skipTagContent.push(new Tag (qn,false));
					return;
				}
				skipTagContent.push(new Tag (qn,true));
				return;
			}
			if((qn.equals("related-article"))) {
				isRelatedArt = true;
				skipTagContent.push(new Tag (qn,true));
				return;
			}
			if((qn.equals("article-title"))&&!isRelatedArt) {
				isTitle = true;
				skipTagContent.push(new Tag (qn,false));
				return;
			}
			if(isTitle 
					&& !qn.equals("bold") 
					&& !qn.equals("italic") 
					&& !qn.equals("underline") 
					&& !qn.equals("monospace")
					&& !qn.equals("named-content") 
					&& !qn.equals("sub") 
					&& !qn.equals("sup") 
					&& !qn.equals("sc") 
					&& !qn.equals("break")
					&& !qn.equals("ext-link") 
					&& !qn.equals("inline-formula") 
					&& !qn.equals("inline-graphic") 
					&& !qn.equals("uri")
					&& !qn.equals("xref")
					&& !qn.startsWith("mml:")) {
				if (!qn.equals("fn")
					&& !skipTagContent.get(skipTagContent.size()-1).tag.equals("fn")) {
					logger.warn("Ignoring unknown element in title: " + qn + " in: " + inpFile.getName());
				}
				skipTagContent.push(new Tag (qn,true));
				return;
			}
			if(qn.equals("contrib-group")) {
				isContribGroup = true;
			}
			if(isContribGroup) {
				if(qn.equals("contrib")) {
					if(attr.getValue("contrib-type").equals("author")) {
						isContribAuth = true;
						if(doc.pmcId!=0) {
							auth = new Author();
							auth.pmcId = doc.pmcId;
							if(doc.pmcId!=0) {auth.pmcId = doc.pmcId;}
						}
						skipTagContent.push(new Tag (qn,false));
						return;
					} 
					else {
						skipTagContent.push(new Tag (qn,true));
						return;
					}
				}
				if(isContribAuth) {
					if(qn.equals("name")) { 
						isAuthName = true;
						skipTagContent.push(new Tag (qn,false));
						return;
					}
					if(isAuthName) {
						if(qn.equals("surname")) {
							skipTagContent.push(new Tag (qn,false));
							return;
						}
						if(qn.equals("given-names")) {
							skipTagContent.push(new Tag (qn,false));
							return;
						}
						skipTagContent.push(new Tag (qn,true));
						return;
					}
					skipTagContent.push(new Tag (qn,true));
					return;
				}
				skipTagContent.push(new Tag (qn,true));
				return;
			}
			if(qn.equals("pub-date")) {
				isPubDate = true;
				skipTagContent.push(new Tag (qn,false));
				return;
			}
			if (isPubDate) {
				if (!qn.equals("year") && !qn.equals("month")) {
					skipTagContent.push(new Tag (qn,true));
					return;
				}
				else {
					skipTagContent.push(new Tag (qn,false));
					return;
				}
			}
			skipTagContent.push(new Tag (qn,true));
			return;
		} // ------------------------------------- /Start tags in article metadata -------------------------------------		

		// ------------------------------------- Start tags for other content of interest -------------------------------------	

		//Citations outside the body and abstract
		if(qn.equals("xref")&&!isFront){
			if(attr.getValue("ref-type") != null && attr.getValue("ref-type").equals("bibr")) {
				isCitation = true;
				processCit(attr);
				skipTagContent.push(new Tag (qn,false));
				return;
			}
			else {
				skipTagContent.push(new Tag (qn,true));
				return;
			}
		}

		//Detect start of main document parts
		if (qn.equals("article")) {
			doc.type = attr.getValue("article-type");
			skipTagContent.push(new Tag (qn,true));
			return;
		}
		if(qn.equals("front")) {
			isFront = true;
			skipTagContent.push(new Tag (qn,true));
			return;
		}
		if(qn.equals("article-meta")) {
			isArticleMeta = true;
			skipTagContent.push(new Tag (qn,false));
			return;
		}
		if(qn.equals("body")) {		
			isBody = true;
			secLevel = 0;
			citCnt = 0;
			cgCnt = 0;
			pCnt = 0;
			sentCnt = 0;
			wordCnt =0;
			charCnt=0;
			lastCitCharCnt = 0;
			skipTagContent.push(new Tag (qn,false));
			return;
		}
		if(qn.equals("ref-list")) {
			isReflist = true;
			this.references = new ArrayList<String>();
			skipTagContent.push(new Tag (qn,false));
			resetExtCont();
			considerPrevCit = false;
			return;
		}

		//Other tags not of interest
		skipTagContent.push(new Tag (qn,true));
		return;
	}
	// ------------------------------------- /Start tags for other content of interest -------------------------------------	

	/**
	 * Handling the end of an XMl tag and if applicable save the information.
	 */
	@SuppressWarnings("finally")
	public void endElement (String ns, String ln, String n) throws SAXException {
		curTag = n;

		// ------------------------------------- End tags body -------------------------------------
		if (isBody) {
			if (n.equals("xref")) {
				if(isCitation) {
					if(maybeAbbrevCit) {
						processAbbrevCit();
						return;
					}
					if(isMultiCit) {
						isMultiCit = false;
						isCitation = false;
						considerPrevCit = false;
						resetExtCont();
						finishTag();
						return;
					}
					isCitation = false;
					considerPrevCit = true;
					finishTag();
					return;
				}
				finishTag();
				return;
			}
			if (n.equals("sec")) {
				if(secCnt.containsKey(secLevel+1)) {
					secCnt.remove(secLevel+1);
				}
				secLevel--;
				finishTag();
				return;
			}
			if (n.equals("p")) {
				finishTag();
				return;
			}
			//SkipElements are not sentence or word marked, thus increase charCnt only
			if (n.equals("table")){finishTag();  isSkipElement = false; return;}
			if (n.equals("disp-formula")){finishTag();  isSkipElement = false; return;}
			if (n.equals("inline-formula")){finishTag();  isSkipElement = false; return;}
			if (n.equals("tex-math")){finishTag();  isSkipElement = false; return;}
			if (n.equals("mml:math")){finishTag();  isSkipElement = false; return;}
			if (n.equals("chem-struct")){finishTag();  isSkipElement = false; return;}

			if(n.equals("body")) {
				isBody = false;
				resetExtCont();
				finishTag();
				return;
			}
			finishTag();
			return;
		}
		// ------------------------------------- /End tags body -------------------------------------

		// ------------------------------------- End tags reference list -------------------------------------
		if (isReflist) {
			if (isReflistItem) {
				if (isRefAuthors) {
					if(n.equals("surname")){
						if (refAuthBuild.length()<40) {refAuthBuild.append(curCont);}
						refAuthorsSet = true;
						resetExtCont();
						finishTag();
						return;
					}
					if (n.equals("collab")) {
						if (!refAuthorsSet) {refAuthBuild.append(curCont);}
						resetExtCont();
						finishTag();
						return;
					}
					if(n.equals("person-group")){
						isRefAuthors = false;
						ref.refAuthorsKey = cleanRefKey(refAuthBuild);
						refAuthBuild = null;
						refAuthorsSet = false;
						finishTag();
						return;
					}
					finishTag();
					return;
				}
				if (isRefArtTitle) {
					if (n.equals("article-title")) {
						isRefArtTitle=false;
						if (!refTransTitleSet) {ref.refTitleKey = cleanRefKey (curCont);}
						refArtTitleSet = true;
						resetExtCont();
						finishTag();
						return;
					}
					finishTag();
					return;
				}
				if (isRefTransTitle) {
					if (n.equals("trans-title")) {
						isRefTransTitle=false;
						ref.refTitleKey = cleanRefKey (curCont);
						refTransTitleSet = true;
						resetExtCont();
						finishTag();
						return;
					}
					finishTag();
					return;
				}
				if (isRefTitle) {
					if (n.equals("title")) {
						isRefTitle=false;
						if(!refTransTitleSet && !refArtTitleSet) {
							ref.refTitleKey = cleanRefKey (curCont);
						}
						refTitleSet = true;
						resetExtCont();
						finishTag();
						return;
					}
					finishTag();
					return;
				}
				if (isRefSource) {
					isRefSource=false;
					if(!refTransTitleSet && !refArtTitleSet && !refTitleSet) {
						ref.refTitleKey = cleanRefKey (curCont);
					}
					resetExtCont();
					finishTag();
					return;
				}
				if(n.equals("pub-id")) {	
					if (isRefPmId) {
						isRefPmId = false;
						try {
							ref.rPmId = Integer.parseInt(trimSB(curCont));
						} 
						catch(NumberFormatException e) {
							throw new SAXException("Error parsing rPmId from: "+ curCont);
						}
						finally {
							resetExtCont();
							finishTag();
							return;
						}
					}
					if(isRefDoi) {
						isRefDoi = false;
						try {
							ref.rDoi = trimSB(curCont);
						}
						catch(Exception e) {
							throw new SAXException("Error parsing rDoi from: "+ curCont);
						}
						finally {
							resetExtCont();
							finishTag();
							return;
						}
					}
					if(isRefPmcId) {
						isRefPmcId = false;
						try {
							ref.rPmcId = Integer.parseInt(trimSB(curCont));
						}
						catch(NumberFormatException e) {
							throw new SAXException("Error parsing rPmcId from: "+ curCont);
						}
						finally {
							resetExtCont();
							finishTag();
							return;
						}
					}
					if(isRefMedID) {
						isRefMedID = false;
						try {
							ref.rMedlineId = trimSB(curCont);
						}
						catch(NumberFormatException e) {
							throw new SAXException("Error parsing rMedlineId from: "+ curCont);
						}
						finally {
							resetExtCont();
							finishTag();
							return;
						}
					}
					finishTag();
					return;
				}
				if (n.equals("collab")){
					if(refAuthBuild==null||refAuthBuild.length()==0) {
						refAuthBuild = new StringBuilder();
						refAuthBuild.append(curCont);
						ref.refAuthorsKey = cleanRefKey(refAuthBuild);
					}
					resetExtCont();
					finishTag();
					return;
				}
				if(n.equals("ref")) {
					isReflistItem = false;
					refArtTitleSet = false;
					refTransTitleSet = false;
					refTitleSet = false;
					refAuthorsSet = false;

					try {
						//dbRefId (auto), pmcid, docRefId, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey
						stmtRef.setInt(1, doc.pmcId);

						stmtRef.setString(2, ref.id.trim());

						if (ref.rPmId!=0) {stmtRef.setInt(3, ref.rPmId);}
						else {stmtRef.setNull(3, java.sql.Types.INTEGER);}

						if (ref.rPmcId!=0) {stmtRef.setInt(4, ref.rPmcId);}
						else {stmtRef.setNull(4, java.sql.Types.INTEGER);}

						if (ref.rMedlineId!=null) {stmtRef.setString(5, ref.rMedlineId.trim());}
						else {stmtRef.setNull(5, java.sql.Types.VARCHAR);}

						if (ref.rDoi!=null) {stmtRef.setString(6, ref.rDoi.trim());}
						else {stmtRef.setNull(6, java.sql.Types.VARCHAR);}

						if (ref.refAuthorsKey!=null) {
							stmtRef.setString(7, ref.refAuthorsKey.trim());}
						else {stmtRef.setNull(7, java.sql.Types.VARCHAR);}

						if (ref.refTitleKey!=null) {
							stmtRef.setString(8, ref.refTitleKey.trim());}
						else {stmtRef.setNull(8, java.sql.Types.VARCHAR);}

						stmtRef.executeUpdate();
					} catch (SQLException e) {	
						throw new SAXException("ERROR (DB-Update - reference): "+e.getMessage());
					}

					//If document contains abbreviated citations. store reference in list for later matching
					if(containsAbbrevCit) {
						references.add(ref.id);
					}
					finishTag();
					return;
				}
				finishTag();
				return;
			} 
			if(n.equals("ref-list")) {
				isReflist = false;
				finishTag();
				return;
			}
		}
		// ------------------------------------- /End tags reference list -------------------------------------

		// ------------------------------------- End tags article metadata -------------------------------------
		if (isArticleMeta) {
			if(isContribGroup) {
				if(isContribAuth) {
					if (isAuthName) {
						if(n.equals("surname")) {
							auth.lastName = curCont.toString();
							resetExtCont();
							finishTag();
							return;
						}
						if(n.equals("given-names")) {
							auth.firstName = curCont.toString();
							resetExtCont();
							finishTag();
							return;
						}
						if(n.equals("name")) { 
							isAuthName = false;
							finishTag();
							return;
						}
						finishTag();
						return;
					}
					if(n.equals("contrib")) {
						isContribAuth = false;
						if(doc.pmcId != 0 && auth.lastName != null) {
							
							if (auth.firstName == null) {
								auth.firstName = "";
							}
							
							try {
								//authId auto increment 
								//pmid
								if(auth.pmcId!=0) {stmtAuth.setInt(1, auth.pmcId);}
								else {stmtAuth.setNull(1, java.sql.Types.INTEGER);}
								//lastName
								stmtAuth.setString(2, auth.lastName.trim());
								//firstName
								stmtAuth.setString(3, auth.firstName.trim());
								stmtAuth.executeUpdate();
								auth = new Author();
							}
							catch (SQLException e) {
								throw new SAXException("ERROR (DB-Update - authors): "+e.getMessage());
							}
						}
						finishTag();
						return;
					}
					finishTag();
					return;
				}
				if(n.equals("contrib-group")) {
					isContribGroup = false;
					finishTag();
					return;
				}
				finishTag();
				return;
			}
			if(isRelatedArt) {
				if(n.equals("related-article")){
					isRelatedArt = false;
					resetExtCont();
					finishTag();
					return;	
				}
				finishTag();
				return;
			}
			if(isTitle) {
				if(n.equals("article-title")){
					isTitle = false;
					doc.title = curCont.toString();
					resetExtCont();
					finishTag();
					return;	
				}
				finishTag();
				return;
			}
			if (n.equals("article-id")) {
				if(isPmcId) {
					isPmcId = false;
					doc.pmcId = Integer.parseInt(trimSB(curCont));
					resetExtCont();
					finishTag();
					return;
				}
				if(isPmId) {
					isPmId = false;
					doc.pmId = Integer.parseInt(trimSB(curCont));
					resetExtCont();
					finishTag();
					return;
				}
				finishTag();
				return;
			}
			if (isPubDate) {
				if (n.equals("year")) {
					try{
						tmpPubYear = Integer.parseInt(trimSB(curCont));
					}
					catch(NumberFormatException e) {}
					finally {
						resetExtCont();
						finishTag();
						return;
					}
				}
				if (n.equals("month")) {
					try {
						tmpPubMonth = Integer.parseInt(trimSB(curCont));
					}catch(NumberFormatException e) {}
					finally {
						resetExtCont();
						finishTag();
						return;
					}
				}
				if (n.equals("pub-date")) {
					if (pubDates==null) {
						pubDates = new LinkedList<PublicationDate>();
						pubDates.add(new PublicationDate (tmpPubYear, tmpPubMonth));
					} else { pubDates.add(new PublicationDate (tmpPubYear, tmpPubMonth));}

					isPubDate = false;
					tmpPubMonth = 99;
					tmpPubYear = 5555;
					finishTag();
					return;
				}
				finishTag();
				return;
			}
			if(isAbstract && n.equals("abstract")) {
				isAbstract = false;
				finishTag();
				return;
			}
			if (n.equals ("article-meta")) {
				isArticleMeta = false;
				if (pubDates != null) {
					if(pubDates.size()>0) {
						Collections.sort(pubDates);
						doc.year = pubDates.get(0).pubYear;
						doc.month = pubDates.get(0).pubMonth;
						pubDates = null;
					}
					else pubDates = null;
				}
				finishTag();
				return;
			}
			finishTag();
			return;
		}// ------------------------------------- /End article metadata -------------------------------------		

		// ------------------------------------- End tags of interest in other parts of the document -------------------------------------	
		if (n.equals("xref")) {
			if(isCitation) {
				if(maybeAbbrevCit) {
					processAbbrevCit();
					return;
				}
				if(isMultiCit) {
					isMultiCit = false;
					isCitation = false;
					considerPrevCit = false;
					resetExtCont();
					finishTag();
					return;
				}
				isCitation = false;
				considerPrevCit = true;
				finishTag();
				return;
			}
			finishTag();
			return;
		}
		if (n.equals("front")) {
			isFront = false;
			finishTag();
			return;
		}
		if (n.equals("sec") && !isFront) {
			if(secCnt.containsKey(secLevel+1)) {
				secCnt.remove(secLevel+1);
			}
			secLevel--;
			finishTag();
			return;
		}
		if (n.equals("p")) {
			finishTag();
			return;
		}
		finishTag();
		return;
	}
	// ------------------------------------- /End tags of interest in other parts of the document -------------------------------------	

	/**
	 * Handling the occurences of character data in the document.
	 */
	public void characters (char[] ch, int start, int length) throws SAXException {		
		if(isBody) {
			//By default no tag is skipped within the body, but skipElements are not sent-word-marked, thus
			//need not to be parsed again. They are considered for increasing charCnt, but not sentCnt or wordCnt
			if (!isSkipElement){
				extractParsedContent (new String (ch, start, length));
				return;
			}
			else {
				//Exception: check for abbreviated citations within skipElements, if such are detected -> extract relevant content	
				if(checkAbbrevCit(length)) {
					extractLiteralContent(ch, start, length);
					return;
				}
				if(!isCitation) {
					charCnt += length;
				}
				return;
			}
		} else if(isAbstract) {
			if (!isSkipElement){
				doc.abstractText.append(extractParsedContent (new String (ch, start, length)));
				return;
			}
		} else {
			if(!skipTagContent.peek().skip) {
				extractLiteralContent(ch, start, length);
				return;
			}
			else {	
				if(checkAbbrevCit(length)) {
					extractLiteralContent(ch, start, length);
					return;
				}
				if(!isCitation && !isArticleMeta && !isFront && !isReflist) {
					charCnt += length;
				}
				return;
			}
		}
	}

	/**
	 * Extract the splitted sentences and words.
	 *
	 * @param tmp the string
	 */
	private String extractParsedContent(String tmp) {
		StringBuffer sentWordParsed = new StringBuffer();
		sentWordMatch = sentWordSplit.matcher(tmp);

		//Evaluate word and sentence boundary markup for increasing respective counts
		while(sentWordMatch.find()) {
			sentWordMatch.appendReplacement(sentWordParsed, "");
			if (sentWordMatch.group().charAt(2)=='W') {
				wordCnt++;
			}
			else {
				sentCnt++;
			}
		}
		sentWordMatch.appendTail(sentWordParsed);

		if(!isCitation && !isArticleMeta && !isFront && !isReflist){
			charCnt += sentWordParsed.length();
		}
		curCont.append(sentWordParsed);

		//Content of current xref-tag is stored in case an abbrev. cit. is possible, but is falsified later on
		//in that case tmpCitCont becomes the start of a possible next abbrev. cit.
		if(maybeAbbrevCit) {
			tmpCitCont.append(sentWordParsed);
		}
		return sentWordParsed.toString();
	} 

	/**
	 * Extract literal contents
	 *
	 * @param ch the characters
	 * @param st the start
	 * @param l the length
	 */
	private void extractLiteralContent(char[] ch, int st, int l) {
		curCont.append(ch, st, l);
		if(maybeAbbrevCit) {
			tmpCitCont.append(ch, st, l);
		}
		if(!isCitation && !isFront && !isReflist) {
			charCnt += l;
		}
		return;
	}

	/**
	 * Check for an abbreviated citation
	 *
	 * @param l the length
	 * @return true if it is an abbreviated citation, otherwise false.
	 */
	private boolean checkAbbrevCit (int l) {
		if( (isCitation) || (considerPrevCit&&(charCnt+l-prevCit.charCnt<=11)) ){
			return true;
		}
		else {
			if(maybeAbbrevCit) {
				return true;
			}
			considerPrevCit = false;
			return false;
		}
	}

	/**
	 * Process a citation.
	 *
	 * @param attr the attributes of the citation tag.
	 * @throws SAXException on error.
	 */
	private void processCit(Attributes attr) throws SAXException {
		String[] rid = attr.getValue("rid").split(" ");
		
//		if(rid[0].equals("B43")) {
//			boolean dummy = true;
//		}
		StringBuilder citSection = new StringBuilder();

		for(int i=1; i<=secLevel; i++) {
			if (citSection.length()!=0) {citSection.append(".");}
			citSection.append(secCnt.get(i));
		}	
		
		if ((charCnt > (lastCitCharCnt + CIT_GRP_CHAR_OFFSET)) || cgCnt == 0) {
			cgCnt++;
		}
		
		lastCitCharCnt = charCnt;
		
		//Store abbrev. citation in format with multiple rids e.g. <xref type="bibr" rid=r1 r2 r3">[1]-[3]</xref>
		if(rid.length>1) {
			for(String r : rid) {
				storeCit(r, ++citCnt, cgCnt, charCnt, wordCnt, sentCnt, pCnt, citSection.toString());
			} 
			isMultiCit=true;
			resetExtCont();
			return;	
		}
		//No previous cit. close enough or multiple rids given so that no abbrev. citation is possible
		if(!considerPrevCit) {
			//Store single cit.
			storeCit(rid[0], ++citCnt, cgCnt, charCnt, wordCnt, sentCnt, pCnt, citSection.toString());
			prevCit = new Citation (doc.pmcId, rid[0], citCnt, cgCnt, charCnt, wordCnt, sentCnt, pCnt, citSection.toString());
			resetExtCont();
			return;	
		}
		//Previous cit. close enough -> current cit. might be part of an abbrev. cit.
		else {
			curCit = new Citation (doc.pmcId, rid[0], 9999, cgCnt, charCnt, wordCnt, sentCnt, pCnt, citSection.toString());
			maybeAbbrevCit = true;
			tmpCitCont = new StringBuilder();
			return;
		}
	}

	/**
	 * Process an abbreviated citation.
	 *
	 * @throws SAXException on error
	 */
	private void processAbbrevCit() throws SAXException {
		multCitMatch = multCitSplit.matcher(trimSB(curCont));
		if (multCitMatch.matches()){
			//Abbreviated cit. detected
			try {
				int startCnt = Integer.parseInt(multCitMatch.group(1));
				int endCnt = Integer.parseInt(multCitMatch.group(2));
				curCit.citCnt = prevCit.citCnt+(endCnt-startCnt);
				curCit.citGrp = cgCnt;
				//Store abbrev. range for later matching with reference list
				abbrevCit.put(prevCit.rid, curCit);
				prevCit=null;
				curCit=null;
				citCnt += endCnt-startCnt;
				containsAbbrevCit = true;
				isCitation = false;
				considerPrevCit = false;
				maybeAbbrevCit = false;
				resetExtCont();
				finishTag();
				return;
			}
			catch (NumberFormatException e) {
				throw new SAXException("ERROR parsing abbrev. citations between: "+prevCit.rid+" and "+curCit.rid);
			}
		}
		//No abbrev. cit.
		else {
			storeCit(curCit.rid, ++citCnt, cgCnt, curCit.charCnt, curCit.wordCnt, curCit.sentCnt, curCit.parCnt, curCit.sec);
			curCit.citCnt = citCnt;
			curCit.citGrp = cgCnt;
			prevCit = curCit;
			curCit = null;
			isCitation = false;
			considerPrevCit = true;
			maybeAbbrevCit = false;
			resetExtCont();
			curCont.append(tmpCitCont);	
			finishTag();
			return;
		}
	}

	/**
	 * Save a citation
	 *
	 * @param rid the reference id
	 * @param ctCnt the citation count
	 * @param citGrp the citation group
	 * @param cCnt the character count
	 * @param wCnt the word count
	 * @param sCnt the sentence count
	 * @param pCnt the paragraph count
	 * @param citSection the section
	 * @throws SAXException 
	 */
	private void storeCit(String rid, int ctCnt, int citGrp, int cCnt, int wCnt, int sCnt, int pCnt, String citSection) throws SAXException {
		try {
			//citId(auto), pmcId, docRefId, dbRefId(auto), cnt, char, word, sent, par, sec
			stmtCit.setInt(1, doc.pmcId);
			stmtCit.setString(2, rid.trim());
			stmtCit.setInt(3, ctCnt);
			stmtCit.setInt(4, citGrp);
			stmtCit.setInt(5, cCnt);
			stmtCit.setInt(6, wCnt);
			stmtCit.setInt(7, sCnt);
			stmtCit.setInt(8, pCnt);
			stmtCit.setString(9, citSection.trim());
			stmtCit.executeUpdate();
		} catch (SQLException e) {
			throw new SAXException("ERROR (DB-Update for citation: "+rid+" cnt "+ctCnt+"): "+e.getMessage());
		}
	}

	/**
	 * Finish handling a tag
	 *
	 * @throws SAXException on error
	 */
	private void finishTag() throws SAXException {
		try {
			if (curTag.equals(skipTagContent.peek().tag)) {
				skipTagContent.pop();
				return;
			}
			else {
				throw new NonMatchingTagsException("Non-matching tags - n: <"+curTag+"> vs. stack: <"+skipTagContent.peek().tag+">");
			}
		}
		catch (Exception e) {
			throw new SAXException(e.getMessage());
		}
	}

	/**
	 * Clean a reference key
	 *
	 * @param inp the input string
	 * @return the cleaned string
	 */
	private String cleanRefKey( StringBuilder inp) {
		//Construction of  artificial reference key for identifying ref. having no other ids
		//-> consists of author names, max length 40 char., non-ASCII char. removed
		if (inp.length()>0) {
			refKeyCleaner = cleanRefKeyP.matcher(inp);
			StringBuffer outp = new StringBuffer();
			while ((outp.length()<40)&&(refKeyCleaner.find())) {
				refKeyCleaner.appendReplacement(outp, "");
			}
			if (outp.length()<40){
				refKeyCleaner.appendTail(outp);
			}
			return (outp.substring(0, Math.min(39, outp.length())).toLowerCase());
		}
		return new String();
	}

	/**
	 * Remove leading and trailing whitespaces from StringBuffer
	 *
	 * @param sb the StringBuffer
	 * @return the StringBuffer with leading and trailing whitespcaes removed
	 */
	private String trimSB(StringBuilder sb) {
		//
		int first, last;

		for (first=0; first<sb.length(); first++)
			if (!Character.isWhitespace(sb.charAt(first)))
				break;

		for (last=sb.length(); last>first; last--)
			if (!Character.isWhitespace(sb.charAt(last-1)))
				break;

		return sb.substring(first, last);
	}

	/**
	 * Resets the external content
	 *
	 */
	private void resetExtCont () {
		curCont = new StringBuilder();
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#resolveEntity(java.lang.String, java.lang.String)
	 */
	public InputSource resolveEntity(String publicId, String systemId) {
		File f = new File(systemId);
		if(!f.exists()) {
			String name = f.getName();
			return new InputSource("file:///"+dtdDir+"/"+name);
		}
		return null;
	}
}
