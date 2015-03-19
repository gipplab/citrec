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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Calculate similarities based on the intersection of the MeSH terms of two documents
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class MeshIntersectionsThread extends Thread {
	protected Connection db;
	protected int doc1;
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		try {
			PreparedStatement stmtSim = db.prepareStatement("INSERT INTO `sim_mesh_intersections` VALUES(?, ?, ?);");
	
	    	Set<Integer> docs2 = new HashSet<Integer>();
	    	for (String desc1 : MeshIntersections.documentDescriptors.get(doc1)) {
	    		docs2.addAll(MeshIntersections.descriptorDocuments.get(desc1));
	    	}
	    	docs2.remove(doc1);
	    	
	    	SortedMap<Double, Vector<Integer>> results = new TreeMap<Double, Vector<Integer>>();
	    	for (int doc2 : docs2) {
	    		int commonDesc = 0;
	    		double value;
		    	Set<String> descs2 = new HashSet<String>();
		    	descs2.addAll(MeshIntersections.documentDescriptors.get(doc2));
	    		for (String desc1 : MeshIntersections.documentDescriptors.get(doc1)) {
	    			if (descs2.contains(desc1)) {
	    				commonDesc++;
	    			}
	    		}
	    		if (commonDesc > 3) {
		    		descs2.addAll(MeshIntersections.documentDescriptors.get(doc1));
		    		value = (double)commonDesc/(double)MeshIntersections.documentDescriptors.get(doc2).size();
		    		if (results.isEmpty() || (value >= results.firstKey()) || (results.size() > 100)) { // last condition???
		    			if (!results.containsKey(value)) {
		    				results.put(value, new Vector<Integer>());
		    			}
		    			results.get(value).add(doc2);
			    		if (results.size() > 10) {
			    			results.remove(results.firstKey());
			    		}
		    		}
	    		}
	    	}
	    	docs2 = null;
	
	    	stmtSim.setInt(1, doc1);
	    	int i = 0;
	    	sql:
		    	for (Double v : results.keySet()) {
		    		stmtSim.setDouble(3, v);
		    		for (int d : results.get(v)) {
		    			i++;
		    			stmtSim.setInt(2, d);
		    			stmtSim.executeUpdate();
		    			if (i > 100) {
		    				break sql;
		    			}
		    		}
		    	}
	    	results = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
