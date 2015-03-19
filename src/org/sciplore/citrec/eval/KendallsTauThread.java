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

package org.sciplore.citrec.eval;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

public class KendallsTauThread extends Thread {
	private String c;
	private String m;
	private byte n;
	private byte sel;
	private Connection db;
	
	public KendallsTauThread(String collection, String method, byte n, byte sel, Connection db) {
		this.c = collection;
		this.m = method;
		this.n = n;
		this.sel = sel;
		this.db = db;
	}
	
	public void run() {
		PreparedStatement stmtDoc;
		PreparedStatement stmtSimMesh;
		PreparedStatement stmtSimMethod;
		int doc;
    	float tmprank;
    	int rank;
    	int cnt;
		int docCnt;
		Vector<Float> results;
		Map<Integer, Ranks> ranks;
		Map<Double, Set<Integer>> methodDocs;
		ResultSet resSimMethod;
		Map<Double, Set<Integer>> meshDocs;
		ResultSet resSimMesh;
		Integer docs[];
		ResultSet resDoc;
		
		try {
			stmtSimMesh = db.prepareStatement("SELECT document2, value FROM sim_" + c + "mesh_lin WHERE document1=? ORDER BY value DESC LIMIT ?");
			docCnt = 0;
			results = new Vector<Float>();
			if (sel == 0) {
				stmtDoc = db.prepareStatement("SELECT document1 FROM " + "sim_" + c + m + " GROUP BY document1 HAVING COUNT(document2) >= ?");
				stmtDoc.setInt(1, n);
			} else {
				stmtDoc = db.prepareStatement("SELECT doc FROM docs_" + c + "selection" + sel);
			}
			resDoc = stmtDoc.executeQuery();
		    while (resDoc.next()) {
		    	doc = resDoc.getInt(1);
		    	if (++docCnt % 10000 == 0) {
		    		System.err.println("n: " + n + "\tMethod: " + m + "\tCollection: " + c + "\tSelection: " + sel + "\t" + docCnt);
		    	}
		    	
		    	ranks = new HashMap<Integer, Ranks>();
		    	
		    	// Read all results of the method from database
		    	stmtSimMethod = db.prepareStatement("SELECT document2, value FROM " + "sim_" + c + m + " WHERE document1=? ORDER BY value DESC LIMIT ?");
		    	stmtSimMethod.setInt(1, doc);
		    	stmtSimMethod.setInt(2, n);
		    	resSimMethod = stmtSimMethod.executeQuery();
		    	methodDocs = new TreeMap<Double, Set<Integer>>();
		    	double value = 0;
		    	while (resSimMethod.next()) {
		    		value = resSimMethod.getDouble("value") * -1; // inverse value for sorting
		    		if (!methodDocs.containsKey(value)) {
		    			methodDocs.put(value, new HashSet<Integer>());
		    		}
		    		methodDocs.get(value).add(resSimMethod.getInt("document2"));
		    	}
		    	resSimMethod.close();
		    	stmtSimMethod.close();

		    	// Generate ranks for all documents of the method
		    	tmprank = 1;
		    	rank = 0;
		    	cnt = 0;
		    	for (double k : methodDocs.keySet()) {
		    		cnt = methodDocs.get(k).size();
		    		tmprank = rank;
		    		tmprank += (double)(cnt+1)/2; // calculate mid rank
		    		for (int d : methodDocs.get(k)) {
		    			if (!ranks.containsKey(d)) {
		    				ranks.put(d, new Ranks());
		    			}
			    		ranks.get(d).a = tmprank;
		    		}
		    		rank = rank + cnt;
		    	}
		    	methodDocs.clear();
		    	methodDocs = null;
		    	
		    	stmtSimMesh.setInt(1, doc);
		    	stmtSimMesh.setInt(2, n);
		    	resSimMesh = stmtSimMesh.executeQuery();
		    	meshDocs = new TreeMap<Double, Set<Integer>>();
		    	while (resSimMesh.next()) {
		    		value = resSimMesh.getDouble("value") * -1;
		    		if (!meshDocs.containsKey(value)) {
		    			meshDocs.put(value, new HashSet<Integer>());
		    		}
		    		meshDocs.get(value).add(resSimMesh.getInt("document2"));
		    	}
		    	resSimMesh.close();
		    	
		    	
		    	tmprank = 1;
		    	rank = 0;
		    	cnt = 0;
		    	for (double k : meshDocs.keySet()) {
		    		cnt = meshDocs.get(k).size();
		    		tmprank = rank;
		    		tmprank += (double)(cnt+1)/2; // calculate mid rank
		    		for (int d : meshDocs.get(k)) {
		    			if (!ranks.containsKey(d)) {
		    				ranks.put(d, new Ranks());
		    			}
			    		ranks.get(d).b = tmprank;
		    		}
		    		rank = rank + cnt;
		    	}
		    	meshDocs.clear();
		    	meshDocs = null;
		    	
		    	float p;
		    	float S = 0;
		    	int dI;
		    	int dJ;
		    	docs = ranks.keySet().toArray(new Integer[0]);
		    	for (int i = 0; i < docs.length; i++) {
		    		for (int j = (i + 1); j < docs.length; j++) {
		    			dI = docs[i];
		    			dJ = docs[j];
		    			p = 0;
		    			if ((ranks.get(dI).a == ranks.get(dJ).a) || (ranks.get(dI).b == ranks.get(dJ).b)) { // Ranks are shared, score is 0
		    				continue;
		    			}
		    			if ((ranks.get(dI).a != 0) && (ranks.get(dI).b != 0) && (ranks.get(dJ).a != 0) && (ranks.get(dJ).b != 0)) { // Case 1
			    			if ((ranks.get(dI).a < ranks.get(dJ).a) && (ranks.get(dI).b > ranks.get(dJ).b)
			    					|| (ranks.get(dI).a > ranks.get(dJ).a) && (ranks.get(dI).b < ranks.get(dJ).b)){
			    				p = 1;
			    			}
		    			} else if ((ranks.get(dI).a != 0) && (ranks.get(dJ).a != 0) && ((ranks.get(dI).b != 0) ^ (ranks.get(dJ).b != 0))) { // Case 2 a
			    			if (((ranks.get(dI).a < ranks.get(dJ).a) && (ranks.get(dI).b == 0))
			    					|| ((ranks.get(dI).a > ranks.get(dJ).a) && (ranks.get(dJ).b == 0))) {
			    				p = 1;
			    			}
		    			} else if ((ranks.get(dI).b != 0) && (ranks.get(dJ).b != 0) && ((ranks.get(dI).a != 0) ^ (ranks.get(dJ).a != 0))) { // Case 2 b
			    			if (((ranks.get(dI).b < ranks.get(dJ).b) && (ranks.get(dI).a == 0))
			    					|| ((ranks.get(dI).b > ranks.get(dJ).b) && (ranks.get(dJ).a == 0))) {
			    				p = 1;
			    			}
		    			} else if (((ranks.get(dI).a != 0) && (ranks.get(dI).b == 0) && (ranks.get(dJ).a == 0) && (ranks.get(dJ).b != 0))
		    					|| ((ranks.get(dI).a == 0) && (ranks.get(dI).b != 0) && (ranks.get(dJ).a != 0) && (ranks.get(dJ).b == 0))) { // Case 3
		    				p = 1;
		    			} else if (((ranks.get(dI).a != 0) && (ranks.get(dJ).a != 0) && (ranks.get(dI).b == 0) && (ranks.get(dJ).b == 0))
		    					|| ((ranks.get(dI).a == 0) && (ranks.get(dJ).a == 0) && (ranks.get(dI).b != 0) && (ranks.get(dJ).b != 0))) { // Case 4
		    				p = (float).5;
		    			}
		    			S += p;
		    		}
		    	}
		    	ranks.clear();
		    	ranks = null;
		    	
		    	S /= n * ((2 * n) - 1);
		    	if (S < 0 || S > 1) {
		    		System.err.println("Komischer Wert!!!");
		    		System.exit(1);
		    	}
		    	S = 1 - S;
		    	results.add(S);
		    }
		    resDoc.close();
		    resDoc = null;
		    Collections.sort(results);
		    float res[] = new float[results.size()];
		    float sum = 0;
		    int i=0;
		    for (float r : results) {
		    	sum += r;
		    	res[i++] = r;
		    }
		    if (i > 0) {
		    	System.out.println("n: " + n + "\tMethod: " + m + "\tCollection: " + c + "\tSelection: " + sel + "\tDocuments: " + docCnt + "\tMean: " + sum/results.size() + "\tMedian: " + res[(i - 1) / 2]);
		    } else {
		    	System.out.println("n: " + n + "\tMethod: " + m + "\tCollection: " + c + "\tSelection: " + sel + "\tDocuments: " + docCnt + "\tNo results!");
		    }
		    results.clear();
		    results = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class Ranks {
	float a = 0;
	float b = 0;

	public String toString() {
		return "{" + a + ", " + b + "}";
	}
}