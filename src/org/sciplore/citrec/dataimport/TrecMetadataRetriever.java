/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Mario Lipinski <lipinski@sciplore.org>

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

import gov.nih.nlm.ncbi.www.soap.eutils.EUtilsServiceSoapProxy;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.ArticleIdType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.ArticleIdTypeIdType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.AuthorType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.DescriptorNameTypeMajorTopicYN;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.EFetchRequest;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.EFetchResult;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.MeshHeadingType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.PubmedArticleType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.QualifierNameType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.QualifierNameTypeMajorTopicYN;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Retrieves document metadata from the <a href="http://www.ncbi.nlm.nih.gov/books/NBK25501/">
 * NLM Entrez Programming Utilities (E-Utilities)</a>
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class TrecMetadataRetriever {
	private static File rootDir;
	private static File trecHtmlDir;
	private static Connection db;
	private static Logger logger = LoggerFactory.getLogger(TrecMetadataRetriever.class);
	private static String ids = "";
	private static int i;
	private final static int BULK_SIZE = 500;
    private static PreparedStatement stmtAuth; 
    private static PreparedStatement stmtDoc; 
    private static PreparedStatement stmtMesh; 
    private static Set<Integer> pmIdsNew;
    private static Set<Integer> pmIdsDb;

	/**
	 * The main program handles the initialization of the configuration, establishes
	 * the database connection and deactivates / reactivates database indices to improve performance.
	 */
	public static void main(String[] args) {
		logger.debug("Trec Metadata Retriever started.");
		try {
			Properties p = Helper.getProperties();

			// root directory where PMC XML files are stored
			rootDir = new File(p.getProperty("rootDir", "."));
			if (!rootDir.isDirectory()) {
				throw new Exception("rootDir is not a valid directory.");
			}

			trecHtmlDir = new File(p.getProperty("trecHtmlDir", rootDir
					+ File.separator + "trec2006" + File.separator + "html"));
			if (!trecHtmlDir.isDirectory()) {
				throw new Exception(
						"Could not find directory with Trec HTML files.");
			}
			// initialize configuration end

			// initialize database connections begin
			Class.forName(p.getProperty("db.driver"));
			db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
			// initialize database connections end
			
			pmIdsDb = new HashSet<Integer>();
			pmIdsNew = new HashSet<Integer>();

			Statement stmt = db.createStatement();
			stmtAuth = db.prepareStatement("INSERT INTO author (pmId, lastname, firstname, initials, suffix, collectivename) VALUES(?, ?, ?, ?, ?, ?);");
			stmtDoc = db.prepareStatement("INSERT INTO document (pmId, title, year, month, journal) VALUES(?, ?, ?, ?, ?);");
			stmtMesh = db.prepareStatement("INSERT INTO mesh (document, descriptor, qualifier, major) VALUES(?, ?, ?, ?);");
			
			ResultSet r = stmt.executeQuery("SELECT DISTINCT pmId FROM document");
			while (r.next()) {
				pmIdsDb.add(r.getInt(1));
			}

			// disable keys on database for faster inserts begin
			stmt.execute("ALTER TABLE `document` DISABLE KEYS");
			stmt.execute("ALTER TABLE `author` DISABLE KEYS");
			stmt.execute("ALTER TABLE `mesh` DISABLE KEYS");
			// disable keys on database for faster inserts end
			
			logger.debug("Initialization finished.");
			logger.info("Processing " + trecHtmlDir.getAbsolutePath() + ".");

			processDirectory(trecHtmlDir);
			
			if (i != 0) {
				retrieveData(ids);
				i = 0;
				ids = "";
			}
			

			logger.info("Finished processing HTML files.");
			logger.info("Re-enabling keys and rebuilding indizes.");

			// re-enable keys on database and rebuild indizes begin
			stmt.execute("ALTER TABLE `document` ENABLE KEYS");
			stmt.execute("ALTER TABLE `author` ENABLE KEYS");
			stmt.execute("ALTER TABLE `mesh` ENABLE KEYS");
			logger.info("Re-enabling keys and rebuilding indizes done.");
			// re-enable keys on database and rebuild indizes end
			
			
			r = stmt.executeQuery("SELECT DISTINCT pmId FROM document");
			while (r.next()) {
				pmIdsDb.add(r.getInt(1));
			}
			
			for (int id : pmIdsNew) {
				if (!pmIdsDb.contains(ids)) {
					System.out.println("Not in DB: " + id);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				db.close();
			} catch (Exception e) {
				logger.error("Could not close database connection.");
			}
		}
		logger.debug("Trec Metadata Import finished.");
	}

	/**
	 * Processes the directory recursively including all sub-directories and
	 * calls a Thread for the XML parser for each XML file.
	 * 
	 * @param dir
	 *            The directory
	 * @throws IOException
	 * @throws CorruptIndexException
	 */
	private static void processDirectory(File dir) throws CorruptIndexException, IOException {

		logger.info("Processing directory " + dir.getAbsolutePath());
		for (File f : dir.listFiles()) {
			if (f.isDirectory()) { // file is a directory
				processDirectory(f); // recurse for sub-directory
			} else if (f.getName().endsWith(".html")) { // file is a XML file to
														// parse
				int pmid = Integer.parseInt(f.getName().substring(0, f.getName().indexOf(".")));
				if (!pmIdsDb.contains(pmid)) {
					if (i++ != 0) {
						ids += ",";
					}
					ids += pmid;
					
					pmIdsNew.add(pmid);
				}
			} else { // there is a file we don't handle
				logger.info("Skipping " + f.getAbsolutePath());
			}
			
			if (i == BULK_SIZE) {
				retrieveData(ids);
				i = 0;
				ids = "";
			}
		}
		logger.info("Finished processing directory " + dir.getAbsolutePath());
	}
	
	private static void retrieveData(String ids) {
		int pmid = 0;
	    try {
			EFetchRequest req = new EFetchRequest();
			req.setId(ids);
			
			EUtilsServiceSoapProxy proxy = new EUtilsServiceSoapProxy();
			EFetchResult res = proxy.run_eFetch(req);
			
			for(PubmedArticleType art : res.getPubmedArticleSet()) {
				pmid=0;
				
				for(ArticleIdType id : art.getPubmedData().getArticleIdList()) {
					if(id.getIdType().equals(ArticleIdTypeIdType.pubmed)) {
						pmid = Integer.parseInt(id.get_value());
					}
				}
				
				stmtDoc.setInt(1, pmid);
				stmtDoc.setString(2, art.getMedlineCitation().getArticle().getArticleTitle().get_value());
				stmtDoc.setString(3, art.getMedlineCitation().getArticle().getJournal().getJournalIssue().getPubDate().getYear());
				stmtDoc.setString(4, art.getMedlineCitation().getArticle().getJournal().getJournalIssue().getPubDate().getMonth());
				stmtDoc.setString(5, art.getMedlineCitation().getArticle().getJournal().getTitle());
				stmtDoc.executeUpdate();
				
				if (art.getMedlineCitation().getArticle().getAuthorList() != null) { 
					for (AuthorType a : art.getMedlineCitation().getArticle().getAuthorList()) {
						stmtAuth.setInt(1, pmid);
						stmtAuth.setString(2, a.getLastName());
						stmtAuth.setString(3, a.getForeName());
						stmtAuth.setString(4, a.getInitials());
						stmtAuth.setString(5, a.getSuffix());
						stmtAuth.setString(6, a.getCollectiveName());
						stmtAuth.executeUpdate();
					}
				} else {
			    	logger.error("No authors for " + pmid);
				}
				
				if(art.getMedlineCitation().getMeshHeadingList() != null) {
					for(MeshHeadingType mh : art.getMedlineCitation().getMeshHeadingList()) {
						int major=0;
						stmtMesh.setInt(1, pmid);
						stmtMesh.setString(2, mh.getDescriptorName().get_value());
						stmtMesh.setNull(3, java.sql.Types.NULL);
						if(mh.getDescriptorName().getMajorTopicYN().equals(DescriptorNameTypeMajorTopicYN.Y)) {
							major = 1;
						}
						stmtMesh.setInt(4, major);
						
						if(mh.getQualifierName() != null) {
							for(QualifierNameType q : mh.getQualifierName()) {
								stmtMesh.setString(3, q.get_value());
								stmtMesh.setInt(4, major);
								if(q.getMajorTopicYN().equals(QualifierNameTypeMajorTopicYN.Y)) {
									stmtMesh.setInt(4, major | 2);
								}
								stmtMesh.executeUpdate();
							}
						} else {
							stmtMesh.executeUpdate();
						}
					}
				}
			}
	    } catch(Exception e) {
	    	logger.error("Error at " + pmid);
	    	e.printStackTrace();
	    }		
	}
}
