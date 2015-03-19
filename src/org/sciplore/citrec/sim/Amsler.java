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

package org.sciplore.citrec.sim;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Calculate the Amsler similarity as described in "Link Information as a 
 * Similarity Measure in Web Classification" by Marco Cristo, Pavel Calado, 
 * Edleno Silva de Moura, Nivio Ziviani und Berthier Ribeiro-Neto. Lecture Notes 
 * in Computer Science, 2003, Volume 2857/2003, 43-55, 
 * DOI: 10.1007/978-3-540-39984-1_4 
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class Amsler {
	/**
	 * Calculate the Amsler similarity
	 *
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Logger logger = LoggerFactory.getLogger(Amsler.class);
		Properties p = Helper.getProperties();
		int intervalSize = 10000; // max number of documents to process in one step.
		
		// initialize database connections begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connections end
		
		Statement stmt = db.createStatement();
		PreparedStatement stmtAmsler = db.prepareStatement("INSERT INTO `similarity_amsler` VALUES(?, ?, ?)");
		
		// Create the table for the results
		stmt.execute("DROP TABLE IF EXISTS sim_amsler");
		stmt.execute("CREATE TABLE IF NOT EXISTS `sim_amsler` (" +
				"`document1` int(11) NOT NULL," +
				"`document2` int(11) NOT NULL," +
				"`value` double NOT NULL," +
				"KEY `document1` (`document1`)," +
				"KEY `document2` (`document2`)" +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
		stmt.execute("ALTER TABLE `sim_amsler` DISABLE KEYS");
		
		int cnt;
		ResultSet res;
		Multimap<Idx, Float> similarities;
		
		res = stmt.executeQuery("SELECT MAX(pmcId) FROM document");
		res.next();
		int maxId = res.getInt(1);
		
		for (int interval = 0; interval < maxId; interval += intervalSize) { // iterate over the batches
			cnt = 0;
			// get batch
			res = stmt.executeQuery("SELECT * FROM similarity_bibco WHERE document1 > " + interval + " AND document1 <= " + (interval + intervalSize));
			similarities = ArrayListMultimap.create();
			while(res.next()) {
		    	if (cnt % 1000 == 0) {
		    		logger.info("Reading Bibliographic Coupling: {}", cnt);
		    	}
		    	similarities.put(new Idx(res.getInt("document1"), res.getInt("document2")), res.getFloat("value"));
		    	cnt++;
		    }
		    res.close();
		    res = stmt.executeQuery("SELECT * FROM similarity_cocit WHERE document1 > " + interval + " AND document1 <= " + (interval + intervalSize));
			cnt = 0;
		    while(res.next()) {
		    	if (cnt % 100 == 0) {
		    		logger.info("Reading Co-Citation: {}", cnt);
		    	}
		    	similarities.put(new Idx(res.getInt("document1"), res.getInt("document2")), res.getFloat("value"));
		    	cnt++;
		    }
		    res.close();
		    res = stmt.executeQuery("SELECT * FROM similarity_linkthrough WHERE document1 > " + interval + " AND document1 <= " + (interval + intervalSize));
			cnt = 0;
		    while(res.next()) {
		    	if (cnt % 100 == 0) {
		    		logger.info("Reading Link Through: {}", cnt);
		    	}
		    	similarities.put(new Idx(res.getInt("document1"), res.getInt("document2")), res.getFloat("value"));
		    	cnt++;
		    }
		    res.close();
		    
		    cnt = 0;
		    for (Idx i : similarities.keySet()) { // itertate over the batch
		    	if (cnt % 1000 == 0) {
		    		logger.info("Processing record: {}", cnt);
		    	}
		    	float value = 0;
		    	for (Float v : similarities.get(i)) { // aggregate values
		    		value += v;
		    	}
		    	// store aggregated similarity for Amsler
		    	stmtAmsler.setInt(1, i.document1);
		    	stmtAmsler.setInt(2, i.document2);
		    	stmtAmsler.setFloat(3, value);
		    	stmtAmsler.executeUpdate();
		    	cnt++;
		    }
		    similarities.clear();
		}
	    
		stmtAmsler.close();
		stmt.execute("ALTER TABLE `sim_amsler` ENABLE KEYS");
		stmt.close();
	}
}

/**
 * Class for indizes used for the Multimap holding similarities.
 */
class Idx implements Comparable<Idx> {
	protected Integer document1;
	protected Integer document2;
	
	public Idx (Integer d1, Integer d2) {
		document1 = d1;
		document2 = d2;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Idx x) {
		if (document1.compareTo(x.document1) == 0) {
			return document2.compareTo(x.document2);
		} else {
			return document1.compareTo(x.document1);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object o) {
		if (o instanceof Idx) {
			return compareTo((Idx)o) == 0;
		}
		return false;
	}
	
	// required for Multimap
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return document1.hashCode()+document2.hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "{" + document1 + "," + document2 + "}";
	}
}

