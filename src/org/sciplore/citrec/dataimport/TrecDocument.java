/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Matt Walters <team@sciplore.org>
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 *
 * @author Matt Walters <a href="mailto:team@sciplore.org">team@sciplore.org</a>
 */
public class TrecDocument implements Runnable{

    // data base stuff
    private ComboPooledDataSource cpds;

    private TrecParser parser;
    private File file;
    private String fileName;
    private List<TrecReference> references = new ArrayList<>();

    public TrecDocument(File file, ComboPooledDataSource cpds) throws Exception {
        this.file = file;
        this.cpds = cpds;
        fileName = file.getName();
       
        parser = new TrecParser();
    }


    public TrecReference addReference(String key, File toParse, String[] value) throws Exception {
        TrecReference r = new TrecReference(key, toParse, value);
        references.add(r);
        return r;
    }

    private void pushToDB() {
    	PreparedStatement stmtRef = null;
    	PreparedStatement stmtCit = null;
        // connect to db
        Connection db = null;
        
        try {
        	db = cpds.getConnection();
            stmtRef = db.prepareStatement("INSERT INTO reference (`document`, `refId`, `refString`, `title`, `authors`, `date`, `booktitle`, `pages`, `volume`, `journal`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmtCit = db.prepareStatement("INSERT INTO citation (`document`, `reference`, `cnt`, `citgrp`, `character`, `word`, `sentence`, `paragraph`, `section`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
	        for (TrecReference ref : references) {
	            int pmid = parsePmid(fileName);
	            stmtRef.setInt(1, pmid);
	            stmtRef.setString(2, ref.key);
	            stmtRef.setString(3, ref.rawRef);
	            stmtRef.setString(4, ref.title);
	            stmtRef.setString(5, ref.authors.toString());
	            stmtRef.setString(6, ref.date);
	            stmtRef.setString(7, ref.bookTitle);
	            stmtRef.setString(8, ref.pages);
	            stmtRef.setString(9, ref.volume);
	            stmtRef.setString(10, ref.journal);
	            stmtRef.executeUpdate();
	
	            for (TrecCitation cit : ref.citations) {
	                stmtCit.setInt(1, pmid); // document
	                stmtCit.setString(2, ref.key); // reference
	                stmtCit.setInt(3, 0); // cnt
	                stmtCit.setInt(4, 0); // citgrp
	                stmtCit.setInt(5, cit.getLocation()); // character
	                stmtCit.setInt(6, 0); // word
	                stmtCit.setInt(7, 0); // sentence
	                stmtCit.setInt(8, 0); // paragraph
	                stmtCit.setString(9, ""); // section
	                stmtCit.executeUpdate();
	            }
	        }
        } catch (Exception e) {
            java.util.logging.Logger.getLogger(TrecDocument.class.getName()).log(Level.SEVERE, null, e);
        } finally {
        	try {
        		if (stmtRef != null) {
        			stmtRef.close();
        		}
        		if (stmtCit != null) {
        			stmtCit.close();
        		}
        		if (db != null) {
        			db.close();
        		}
			} catch (SQLException e) {
	            java.util.logging.Logger.getLogger(TrecDocument.class.getName()).log(Level.SEVERE, null, e);
			}
        }
         
         
    }

    private int parsePmid(String fileName) {
        return Integer.parseInt(fileName.substring(0, fileName.length() - 5));
    }

    @Override
    public void run() {
        try {
            Map<String, String[]> keyRefMap = parser.getKeyRefMap(file);
            for (String key : keyRefMap.keySet()) {
                String[] values = keyRefMap.get(key);
                addReference(key, file, values);
            }
            
            keyRefMap = null;
            pushToDB();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
