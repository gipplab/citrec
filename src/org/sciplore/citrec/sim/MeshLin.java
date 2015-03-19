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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;

import org.sciplore.citrec.Helper;

/**
 * Calculate document similarity based on Lin's measure of information content applied to MeSH terms
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */

public class MeshLin {
	private final static int NUM_RESULTS = 100;
//	private static ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
//	private static Map<String, Map<String, Double>> descSimCache = new HashMap<String, Map<String,Double>>();
	protected static Map<String, Vector<Term>> meshDesc = new HashMap<String, Vector<Term>>();
	protected static Map<String, Double> meshId = new HashMap<String, Double>();
	protected static Map<Integer, Vector<String>> docDesc = new HashMap<Integer, Vector<String>>();
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException, IOException {
		Properties p = Helper.getProperties();
		int NUM_THREADS = Integer.parseInt(p.getProperty("numThreads"));
		Class.forName(p.getProperty("db.driver"));

		Connection dbs[] = new Connection[NUM_THREADS];
		PreparedStatement stmtSim[] = new PreparedStatement[NUM_THREADS];
    	MeshLinThread t[] = new MeshLinThread[NUM_THREADS];
    	String simQuery = "INSERT INTO sim_mesh_lin VALUES";
    	
    	for (int i = 0; i < NUM_RESULTS; i++) {
    		simQuery += (i != 0 ? "," : "") + " (?, ?, ?)";
    	}

		for (int i = 0; i < NUM_THREADS; i++) {
			dbs[i] = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
			stmtSim[i] = dbs[i].prepareStatement(simQuery);
		}
		
		Statement stmt = dbs[0].createStatement();

		stmt.execute("CREATE TABLE IF NOT EXISTS `sim_mesh_lin` (" +
				"`document1` int(11) NOT NULL," +
				"`document2` int(11) NOT NULL," +
				"`value` double NOT NULL," +
				"KEY `document1` (`document1`)," +
				"KEY `document2` (`document2`)" +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
		stmt.execute("ALTER TABLE `sim_mesh_lin` DISABLE KEYS");

		Statement stmtMeshTree = dbs[0].createStatement();
		ResultSet resMeshTree = stmtMeshTree.executeQuery("SELECT DISTINCT id, descriptor AS term, ic FROM meshtree");
//		ResultSet resMeshTree = stmtMeshTree.executeQuery("SELECT t.id, term, ic FROM meshtree_terms t LEFT JOIN meshtree m ON t.id=m.id"); // not DISTINCT because of case sensitivity
		while (resMeshTree.next()) {
			if (!meshDesc.containsKey(resMeshTree.getString("term"))) {
				meshDesc.put(resMeshTree.getString("term"), new Vector<Term>());
			}
			meshDesc.get(resMeshTree.getString("term")).add(new Term(resMeshTree.getString("id"), resMeshTree.getDouble("ic")));
			meshId.put(resMeshTree.getString("id"), resMeshTree.getDouble("ic"));
		}
		resMeshTree.close();
		stmtMeshTree.close();
		
		Statement stmtDoc = dbs[0].createStatement();
		ResultSet resDoc = stmtDoc.executeQuery("SELECT DISTINCT document, descriptor FROM mesh WHERE major != 0");
		while (resDoc.next()) {
			if (!docDesc.containsKey(resDoc.getInt("document"))) {
				docDesc.put(resDoc.getInt("document"), new Vector<String>());
			}
			docDesc.get(resDoc.getInt("document")).add(resDoc.getString("descriptor"));
		}
		resDoc.close();
		stmtDoc.close();
		
		int i = 0;
		int j = 0;
		int cnt = 0;
		
		Statement stmtDoneDocs = dbs[0].createStatement();
		ResultSet resDoneDoc = stmtDoneDocs.executeQuery("SELECT DISTINCT document1 FROM sim_mesh_lin");
		TreeSet<Integer> doneDocs = new TreeSet<Integer>();
		while (resDoneDoc.next()) {
			doneDocs.add(resDoneDoc.getInt("document1"));
		}
		resDoneDoc.close();
		stmtDoneDocs.close();

//		Statement stmtTodoDocs = dbs[0].createStatement();
//		ResultSet resTodoDoc = stmtTodoDocs.executeQuery("SELECT DISTINCT id FROM todo_sim_mesh_lin ORDER BY id LIMIT " + args[0] + ",20000");
//		TreeSet<Integer> todoDocs = new TreeSet<Integer>();
//		while (resTodoDoc.next()) {
//			todoDocs.add(resTodoDoc.getInt("id"));
//		}
//		resTodoDoc.close();
//		stmtTodoDocs.close();

		for (Integer d : docDesc.keySet()) {
//			if (!todoDocs.contains(d) || doneDocs.contains(d)) {
			if (doneDocs.contains(d)) {
				continue;
			}
			
			if (++cnt % 100 == 0) {
				System.err.println(cnt);
		    	System.gc();
			}
			
			System.out.println(d);

			while (true) {
				if (i == NUM_THREADS) {
	    			i = 0;
	    		}

	    		if (t[i] == null || !t[i].isAlive()) {
	    			t[i] = null;
	    			t[i] = new MeshLinThread(stmtSim[i], d);
	    			t[i].start();
	    			i++;
	    			break;
	    		}
	    		if (j == NUM_THREADS) {
			    	try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					j = 0;
	    		}

	    		i++;
	    		j++;
	    	}
		}
		
		for (Thread thread : t) {
			if (thread != null) {
				thread.join();
			}
		}
		
		stmt.execute("ALTER TABLE `sim_mesh_lin` ENABLE KEYS");
		stmt.close();
		
		for (Connection d : dbs) {
			d.close();
		}
	}
	
//	protected static Double getCachedDescSim(String desc1, String desc2) {
//		if (desc1.compareTo(desc2) > 0) {
//			String tmp = desc2;
//			desc2 = desc1;
//			desc1 = tmp;
//		}
//		rwl.readLock().lock();
//		if (descSimCache.containsKey(desc1) && descSimCache.get(desc1).containsKey(desc2)) {
//			double v = descSimCache.get(desc1).get(desc2);
//			rwl.readLock().unlock();
//			return v;
//		}
//		rwl.readLock().unlock();
//		return null;
//	}
//
//	protected static void setCachedDescSim(String desc1, String desc2, double value) {
//		if (desc1.compareTo(desc2) > 0) {
//			String tmp = desc2;
//			desc2 = desc1;
//			desc1 = tmp;
//		}
//		
//		rwl.writeLock().lock();
//		if (!descSimCache.containsKey(desc1)) {
//				descSimCache.put(desc1, new TreeMap<String, Double>());
//		}
//		descSimCache.get(desc1).put(desc2, value);
//		rwl.writeLock().unlock();
//	}
}

class Term {
	private String id;
	private double ic;
	
	public Term(String id, double ic) {
		this.id = id;
		this.ic = ic;
	}
	
	protected double compareToLin(Term t) {
		if(id.equals(t.id)) {
			return 1;
		}
		String ids[] = id.split("\\.");
		String ids2[] = t.id.split("\\.");
		if (ids.length < 1 || ids2.length < 1 || !ids[0].equals(ids2[0])) {
			return 0;
		}
		int i = 1;
		String lcs = ids[0];
		while (ids.length > i && ids2.length > i && ids[i].equals(ids2[i])) {
			lcs += "." + ids[i];
			i++;
		}
		
		return (2 * MeshLin.meshId.get(lcs)) / (ic + t.ic);
	}
	
	public String toString() {
		return id + " => " + ic;
	}
}