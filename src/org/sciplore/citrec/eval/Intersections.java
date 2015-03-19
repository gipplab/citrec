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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.sciplore.citrec.Helper;
/**
 * Evaluation utility that performs a set-based comparison of the top-k 
 * documents ranked according to a selected similarity measure.
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 *
 */
public class Intersections {

	/**
	 * 
	 *
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Properties p = Helper.getProperties();
		Class.forName("com.mysql.jdbc.Driver");
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		
		PreparedStatement stmtSimMesh = db.prepareStatement("SELECT document2 FROM sim_mesh_lin WHERE document1=? ORDER BY value DESC LIMIT ?");
		
		String methods[] = { "amsler", "amsler_rel", "bibco", "bibco_rel", "cocit", "cocit_relative", "context_avg", "context_pow1", "context_pow2", "context_rt2", "context_rt5", "context_rt10", "context_sum", "cpa_2simple_tree", "cpa_5simple_tree", "cpa_10simple_tree", "cpa_pow1", "cpa_pow2", "cpa_rt2", "cpa_rt5", "cpa_rt10", "cpa_simple", "cpa_simple_2tree", "cpa_simple_5tree", "cpa_simple_10tree", "cpa_simple_tree", "cpa_sum", "cpa_tree", "linkthrough", "luceneb_title_abstract", "luceneb_title_abstract_text", "lucene_abstract", "lucene_text", "lucene_title" };
		int intersections;
		int maxK = 20;
		int doc;
		int iter = 2;
		
		int k = maxK;
		while (iter <= 10) {
			while (k > 0) {
				for (String m : methods) {
					PreparedStatement stmtSimMethod = db.prepareStatement("SELECT document2 FROM sim_" + m + " WHERE document1=? ORDER BY value DESC LIMIT ?");
					PreparedStatement stmtDoc = db.prepareStatement("SELECT s.document1 AS id FROM testdocs_maxcocit_" + iter + " m LEFT JOIN sim_" + m + " s ON s.document1=m.id WHERE EXISTS (SELECT 1 FROM sim_mesh_lin WHERE document1=s.document1) GROUP BY s.document1 HAVING COUNT(s.document2) >= ?");
	//				PreparedStatement stmtDoc = db.prepareStatement("SELECT id FROM testdocs_x_" + k);
					int docCnt = 0;
					Vector<Float> results = new Vector<Float>();
					stmtDoc.setInt(1, k);
					ResultSet resDoc = stmtDoc.executeQuery();
				    while (resDoc.next()) {
				    	doc = resDoc.getInt("id");
			//		for (int doc : docs) {
				    	if (++docCnt % 100 == 0) {
				    		System.err.println(docCnt);
				    	}
				    	
				    	intersections = 0;
				    	
		//		    	System.out.println(doc);
			
				    	stmtSimMesh.setInt(1, doc);
				    	stmtSimMesh.setInt(2, k);
				    	ResultSet rMesh = stmtSimMesh.executeQuery();
				    	Set<Integer> meshSims = new HashSet<Integer>();
				    	while (rMesh.next()) {
				    		meshSims.add(rMesh.getInt("document2"));
				    	}
	//			    	System.out.println(meshSims);
						
				    	stmtSimMethod.setInt(1, doc);
				    	stmtSimMethod.setInt(2, k);
				    	ResultSet rMethod = stmtSimMethod.executeQuery();
				    	while (rMethod.next()) {
	//			    		System.out.println(rMethod.getInt("document2"));
				    		if (meshSims.contains(rMethod.getInt("document2"))) {
				    			intersections++;
				    		}
				    	}
				    	
				    	results.add((float)intersections / k);
				    	
	//			    	System.out.println(intersections);
		//		    	System.out.println();
				    }
			//	    resDoc.close();
				    
				    float res[] = new float[results.size()];
				    float sum = 0;
				    int i=0;
				    for (float r : results) {
				    	sum += r;
				    	res[i++] = r;
				    }
				    if (i > 0) {
				    	System.out.println("i: " + iter + "\tk: " + k + "\tMethod: " + m + "\tDocuments: " + docCnt + "\tMean: " + sum/results.size() + "\tMedian: " + res[(i - 1) / 2]);
				    } else {
				    	System.out.println("i: " + iter + "\tk: " + k + "\tMethod: " + m + "\tDocuments: " + docCnt + "\tNo results!");
				    }
				}
				k -= 1;
			}
			k = maxK;
			iter++;
		}
	}
}
