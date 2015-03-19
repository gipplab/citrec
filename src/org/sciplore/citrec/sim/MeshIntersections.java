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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate similarities based on the intersection of the MeSH terms of two documents
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */

public class MeshIntersections {
	public static Map<String, Set<Integer>> descriptorDocuments = new HashMap<String, Set<Integer>>();
	public static Map<Integer, Set<String>> documentDescriptors = new HashMap<Integer, Set<String>>();
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Logger logger = LoggerFactory.getLogger(MeshIntersections.class);
		Properties p = Helper.getProperties();
		int numThreads = Integer.parseInt(p.getProperty("numThreads"));
		Connection dbs[] = new Connection[numThreads];

		// initialize database connections begin
		Class.forName(p.getProperty("db.driver"));
		for (int i = 0; i < dbs.length; i++) {
			dbs[i] = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));

		}
		// initialize database connections end

    	MeshIntersectionsThread t[] = new MeshIntersectionsThread[numThreads];

		Statement stmtdoc = dbs[0].createStatement();
		ResultSet resMesh = stmtdoc.executeQuery("SELECT document, descriptor FROM mesh");

		while (resMesh.next()) {
			int doc = resMesh.getInt("document");
			String desc = resMesh.getString("descriptor");
			
			if (!descriptorDocuments.containsKey(desc)) {
				descriptorDocuments.put(desc, new HashSet<Integer>());
			}
			descriptorDocuments.get(desc).add(doc);

			if (!documentDescriptors.containsKey(doc)) {
				documentDescriptors.put(doc, new HashSet<String>());
			}
			documentDescriptors.get(doc).add(desc);
		}
		
		int cnt = 0;
		int cnt2 = 0;
		
		// For improvement use documentDescriptors keySet
		ResultSet resDoc = stmtdoc.executeQuery("SELECT DISTINCT pmcId FROM document WHERE pmcId != 0 AND EXISTS (SELECT 1 FROM mesh WHERE document=pmcId)");
		
		Statement stmt = dbs[0].createStatement();
		stmt.execute("DROP TABLE IF EXISTS sim_mesh_intersections");
		stmt.execute("CREATE TABLE IF NOT EXISTS `sim_mesh_intersections` (" +
				"`document1` int(11) NOT NULL," +
				"`document2` int(11) NOT NULL," +
				"`value` double NOT NULL," +
				"KEY `document1` (`document1`)," +
				"KEY `document2` (`document2`)" +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
		stmt.execute("ALTER TABLE `sim_mesh_intersections` DISABLE KEYS");

		int i = 0;
	    while (resDoc.next()) {
	    	if (++cnt % 100 == 0) {
	    		logger.info("MeSH Intersections: {}", cnt);
	    	}
	    	
    		int j = 0;
	    	while (true) {
	    		if (i == numThreads) {
	    			i = 0;
	    		}

	    		if (t[i] == null || !t[i].isAlive()) {
	    			t[i] = new MeshIntersectionsThread();
	    			t[i].doc1 = resDoc.getInt("pmcId");
	    			t[i].db = dbs[i];
	    			t[i].start();
	    			i++;
	    			break;
	    		}
	    		if (j == numThreads) {
			    	try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					j = 0;
	    		}

	    		i++;
	    		j++;
	    	}

	    	if (cnt % 100 == 0) {
		    	System.gc();
	    	}
	    }
	    resDoc.close();
		stmt.execute("ALTER TABLE `sim_mesh_intersections` ENABLE KEYS");
		stmt.close();
		stmtdoc.close();
		
		for (Connection db : dbs) {
			db.close();
		}

		System.out.println(cnt2);
	}
}
