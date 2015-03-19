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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Imports the MeSH tree from XML file into the database.
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class MeshXml {
	private static Logger logger = LoggerFactory.getLogger(MeshXml.class);
	
	/**
	 * Main program for importing the MeSH tree from XML file into the database
	 *
	 * @param args This program takes exactly one argument and points to the 
	 * 			filename of the MeSH data. The data is available from 
	 * 			<a href="http://www.nlm.nih.gov/mesh/filelist.html">http://www.nlm.nih.gov/mesh/filelist.html</a>
	 * 			and labeled MeSH Trees (2011 MeSH Trees for the 2011 version with
	 * 			filename desc2011.xml.
	 * @throws ParserConfigurationException 
	 * @throws SAXException 
	 */
	public static void main(String args[]) throws ParserConfigurationException, SAXException {
		Connection db = null;
		try {
			logger.info("Initializing.");

			Properties p = Helper.getProperties();
			
			// XML file containing data
			String meshFile = "";
			
			if (args.length == 1 && new File(args[0]).canRead()) {
				meshFile = args[0];
			} else {
				String msg = "The argument is not a valid file.";
				logger.error(msg);
				System.err.println(msg);
				System.exit(-1);
			}
			 
			// initialize database connection begin
			Class.forName(p.getProperty("db.driver"));
			db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
			// initialize database connection end
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	        dbf.setAttribute("http://xml.org/sax/features/namespaces", true);
	        dbf.setAttribute("http://xml.org/sax/features/validation", false);
	        dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", false);
	        dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

	        dbf.setNamespaceAware(true);
	        dbf.setIgnoringElementContentWhitespace(false);
	        dbf.setIgnoringComments(false);
	        dbf.setValidating(false);


			Statement stmt = db.createStatement();
			// disable keys on database for faster inserts begin
			stmt.execute("ALTER TABLE `meshtree` DISABLE KEYS");
			// disable keys on database for faster inserts end

			DocumentBuilder docb = dbf.newDocumentBuilder();
			logger.info("Parsing XML file.");
			Document dom = docb.parse(meshFile);
			NodeList nl = dom.getDocumentElement().getElementsByTagName("DescriptorRecord");
			
			PreparedStatement stmtMeshTree = db.prepareStatement("INSERT INTO meshtree (id, descriptor) VALUES(?, ?);");
			PreparedStatement stmtMeshTerm = db.prepareStatement("INSERT INTO meshtree_terms (id, term) VALUES(?, ?);");
			System.out.println(nl.getLength());
			for (int i = 0; i < nl.getLength(); i++) {
				Element e = (Element)nl.item(i);
				String dn = ((Element)e.getElementsByTagName("DescriptorName").item(0)).getElementsByTagName("String").item(0).getTextContent();
				System.out.println(dn);
				stmtMeshTree.setString(2, dn);
				NodeList treeNumber = e.getElementsByTagName("TreeNumber");
				NodeList terms = e.getElementsByTagName("Term");
				for (int j = 0; j < treeNumber.getLength(); j++) {
					String tn = treeNumber.item(j).getTextContent().trim();
					System.out.println(" " + tn);
					stmtMeshTree.setString(1, tn);
					stmtMeshTree.executeUpdate();
					stmtMeshTerm.setString(1, tn);
					for (int k = 0; k < terms.getLength(); k++) {
						String term = ((Element)terms.item(k)).getElementsByTagName("String").item(0).getTextContent();
						System.out.println("  " + term);
						stmtMeshTerm.setString(2, term);
						stmtMeshTerm.executeUpdate();
					}
				}
				if (i % 1000 == 0) {
					logger.debug("{} entries processed.", i);
				}
			}
			
			logger.info("Done. Re-enabling keys and rebuilding index.");
			// re-enable keys on database and rebuild indizes begin
			stmt.execute("ALTER TABLE `meshtree` ENABLE KEYS");
			// re-enable keys on database and rebuild indizes end

			stmt.close();
			stmtMeshTerm.close();
			stmtMeshTree.close();
		} catch (SQLException e) {
			logger.error("An error occured while communicating with the database. {}", e.getMessage());
		} catch (IOException e) {
			logger.error("An error occured during I/O operations. {}" ,e.getMessage());
		} catch (ClassNotFoundException e) {
			logger.error("Could not load database driver. {}", e.getMessage());
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					logger.error("Could not close database connection. {}", e.getMessage());
				}
			}
		}
	}
}
