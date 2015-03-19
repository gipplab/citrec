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
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main program for importing data from PubMed Central XML files. 
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class PmcXmlImport {
	private static int numThreads;
	private static File rootDir;
	private static File pmcXmlDir;
	private static File pmcTxtDir;
	private static File indexDir;
	private static File dtdDir;
	private static IndexWriter indexWriter;
	private static Connection db[];
	private static Thread t[];
	private static Logger logger = LoggerFactory.getLogger(PmcXmlImport.class);
	private static int i = 0;
	
	/**
	 * The main program handles the initialization of the configuration, 
	 * database connection and deactivating indices for improving performance 
	 * and reactivating and rebuilding them when finished. 
	 */
	public static void main(String[] args) {
		logger.debug("PMC XML Import started.");
		try {
			Properties p = Helper.getProperties();
			
			// initialize configuration begin
			// number of concurrent threads
			numThreads = Integer.parseInt(p.getProperty("PmcXmlImport.numThreads"));
			if (numThreads < 1) {
				numThreads = 1;
			}
			
			// root directory where PMC XML files are stored
			rootDir = new File(p.getProperty("rootDir", "."));
			if (!rootDir.isDirectory()) {
				throw new Exception("rootDir is not a valid directory.");
			}
			
			pmcXmlDir = new File(p.getProperty("pmcXmlDir", rootDir + File.separator + "pmc" + File.separator + "xml"));
			if (!pmcXmlDir.isDirectory()) {
				throw new Exception("Could not find directory with PMC XML files.");
			}
			
			pmcTxtDir = new File(p.getProperty("pmcTxtDir", rootDir + File.separator + "pmc" + File.separator + "txt"));
			if (!pmcTxtDir.isDirectory() && !pmcTxtDir.mkdirs()) {
				throw new Exception("Could not find or create directory for PMC TXT files.");
			}
			
			indexDir = new File(p.getProperty("indexDir", rootDir + File.separator + "index"));
			if (!indexDir.isDirectory() && !indexDir.mkdir()) {
				throw new Exception("Could not find index directory.");
			}

			dtdDir = new File(p.getProperty("dtdDir", rootDir + File.separator + "dtd"));
			if (!dtdDir.isDirectory()) {
				throw new Exception("Could not find directory with DTDs.");
			}
			// initialize configuration end
			

			// initialize database connections begin
			Class.forName(p.getProperty("db.driver"));
			db = new Connection[numThreads];
			
			for (int i = 0; i < numThreads; i++) {
				db[i] = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
			}
			// initialize database connections end
			
			assert db.length > 0;
	
			Statement stmt = db[0].createStatement();

			// disable keys on database for faster inserts begin
			stmt.execute("ALTER TABLE `document` DISABLE KEYS");
			stmt.execute("ALTER TABLE `author` DISABLE KEYS");
			stmt.execute("ALTER TABLE `citation` DISABLE KEYS");
			stmt.execute("ALTER TABLE `reference` DISABLE KEYS");
			// disable keys on database for faster inserts end
			logger.debug("Initialization finished.");
			logger.info("Processing " + pmcXmlDir.getAbsolutePath() + " with " + numThreads + " threads.");

			indexWriter = new IndexWriter(new NIOFSDirectory(indexDir), new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34)));

			t = new PmcXmlParser[numThreads];
			processDirectory(pmcXmlDir);
			
			for (Thread thread : t) {
				if (thread != null) {
					thread.join();
				}
			}
			
			logger.info("Finished processing XML files.");

			indexWriter.optimize();
			indexWriter.close();
			
			logger.info("Re-enabling keys and rebuilding indizes.");
			
			// re-enable keys on database and rebuild indizes begin
			stmt.execute("ALTER TABLE `document` ENABLE KEYS");
			stmt.execute("ALTER TABLE `author` ENABLE KEYS");
			stmt.execute("ALTER TABLE `citation` ENABLE KEYS");
			stmt.execute("ALTER TABLE `reference` ENABLE KEYS");
			logger.info("Re-enabling keys and rebuilding indizes done.");
			// re-enable keys on database and rebuild indizes end

			logger.info("Deleting author data for ignored documents.");
			stmt.execute("DELETE FROM `author` WHERE NOT EXISTS (SELECT 1 FROM document d WHERE d.pmcId=author.pmcId)");
			logger.info("Deleting citation data for ignored documents.");
			stmt.execute("DELETE FROM `citation` WHERE NOT EXISTS (SELECT 1 FROM document WHERE pmcId=document)");
			logger.info("Deleting reference data for ignored documents.");
			stmt.execute("DELETE FROM `reference` WHERE NOT EXISTS (SELECT 1 FROM document WHERE pmcId=document)");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				for (Connection d : db) {
					d.close();
				}
			} catch (Exception e) {
				logger.error("Could not close database connection.");
			}
		}
		logger.debug("PMC XML Import finished.");
	}
	
	/**
	 * Processes the directory recursively including all sub-directories and 
	 * calls a Thread for the XML parser for each XML file.
	 *
	 * @param dir The directory
	 * @throws IOException 
	 * @throws CorruptIndexException 
	 */
	private static void processDirectory(File dir) throws CorruptIndexException, IOException {
		int i = 0;
		int j = 0;
		
		logger.info("Processing directory " + dir.getAbsolutePath());
		for (File f : dir.listFiles()) {
			if (f.isDirectory()) { // file is a directory
				processDirectory(f); // recurse for sub-directory
			} else if (f.getName().endsWith(".nxml")) { // file is a XML file to parse
				// assign file to a thread begin
				while (true) {
					if (i == numThreads) {
		    			i = 0;
		    		}

		    		if (t[i] == null || !t[i].isAlive()) {
		    			t[i] = null;
		    			logger.info("Processing file " + f.getName());
		    			t[i] = new PmcXmlParser(f, db[i], indexWriter, pmcXmlDir, pmcTxtDir, dtdDir);
		    			t[i].start(); // run Thread
		    			i++;
		    			break;
		    		}
		    		if (j == numThreads) {
				    	try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						j = 0;
		    		}

		    		i++;
		    		j++;
		    	}
				// assign file to a thread end
				
				if (++PmcXmlImport.i % 500 == 0) {
					indexWriter.commit();
					System.gc();
				}
				
			} else { // there is a file we don't handle
				logger.info("Skipping " + f.getAbsolutePath());
			}
		}
		logger.info("Finished processing directory " + dir.getAbsolutePath());
	}
}
