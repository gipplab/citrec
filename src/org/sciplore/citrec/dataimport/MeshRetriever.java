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
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.DescriptorNameTypeMajorTopicYN;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.EFetchRequest;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.EFetchResult;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.MeshHeadingType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.PubmedArticleType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.QualifierNameType;
import gov.nih.nlm.ncbi.www.soap.eutils.efetch_pubmed.QualifierNameTypeMajorTopicYN;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves MeSH headings for articles stored in the database from Entrez web service .
 * 
 * This program is intentionally single-threaded to meet the usage restrictions set out 
 * for the web service.
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class MeshRetriever {
	private final static int BULK_SIZE = 500;
	private static Logger logger = LoggerFactory.getLogger(MeshRetriever.class);
	
	/**
	 * Main program for retrieving MeSH headings from web service 
	 *
	 * @param args (not used)
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		Properties p = Helper.getProperties();
		
		// initialize database connection begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connection end		

	    String ids;

	    Statement stmtdoc = db.createStatement();
	    PreparedStatement stmtMesh = db.prepareStatement("INSERT INTO mesh (document, descriptor, qualifier, major) VALUES(?, ?, ?, ?);"); 
		ResultSet resdoc = stmtdoc.executeQuery("SELECT DISTINCT pmcId FROM document WHERE pmcId != 0 AND NOT EXISTS (SELECT 1 FROM mesh WHERE document=pmcId)");
		
		int cnt = 0;
		
		/*
		 * Retrieve all documents that do not have MeSH data in the database
		 * and process them in bulks of size BULK_SIZE
		 */
	    while(resdoc.next()) {
		    ids = resdoc.getString("pmcId");
		    cnt++;
		    for(int j = 1; resdoc.next() && j < BULK_SIZE; j++) {
		    	ids += ",";
			    ids += resdoc.getString("pmcId");
			    cnt++;
		    }
			
		    try {
				EFetchRequest req = new EFetchRequest();
				req.setId(ids);
				
				EUtilsServiceSoapProxy proxy = new EUtilsServiceSoapProxy();
				EFetchResult res = proxy.run_eFetch(req);
				
				for(PubmedArticleType art : res.getPubmedArticleSet()) {
					int pmid=0;
					
					for(ArticleIdType id : art.getPubmedData().getArticleIdList()) {
						if(id.getIdType().equals(ArticleIdTypeIdType.pubmed)) {
							pmid = Integer.parseInt(id.get_value());
						}
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
		    	e.printStackTrace();
		    }
		    logger.info("{} documents processed.", cnt);
		}
	}
}
