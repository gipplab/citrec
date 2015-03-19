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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
/**
 * Calculate document similarity based on Lin's measure of information content applied to MeSH terms
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */

public class MeshLinThread extends Thread {
	private PreparedStatement stmtSim;
	private int doc;
	
	public MeshLinThread(PreparedStatement stmtSim, int doc) {
		this.stmtSim = stmtSim;
		this.doc = doc;
	}
	
	public void run() {
		try {
			SortedMap<Double, Set<Integer>> results = new TreeMap<Double, Set<Integer>>();
			
			double simDesc;
			double simDoc;
			double tmpVal;
			int cnt;

			for (Integer d : MeshLin.docDesc.keySet()) {
				if (doc == d) {
					continue;
				}
				
				simDoc = 0;
				cnt = 0;
				for (String d2 : MeshLin.docDesc.get(d)) {
					simDesc = 0.;
					for (String d1 : MeshLin.docDesc.get(doc)) {
						tmpVal = 0;
//						if (MeshLin.getCachedDescSim(d2, d1) == null) {
							if (MeshLin.meshDesc.containsKey(d1)) {
								for (Term t1 : MeshLin.meshDesc.get(d1)) {
									if (MeshLin.meshDesc.containsKey(d2)) {
										for (Term t2 : MeshLin.meshDesc.get(d2)) {
											// calc similarity between t1 and t2
											tmpVal = Math.max(tmpVal, t1.compareToLin(t2));
										}
									} else {
										System.err.println("Not in MeSH tree: " + d2);
									}
								}
							} else {
								System.err.println("Not in MeSH tree: " + d1);
							}
//							MeshLin.setCachedDescSim(d2, d1, tmpVal);
//						} else {
//							tmpVal = MeshLin.getCachedDescSim(d2, d1);
//						}
//						System.out.println(resMesh.getString("descriptor") + " " + d1 + ": " + tmpVal);
						simDesc = Math.max(simDesc, tmpVal);
					}
					// similarity between two descriptors
					simDoc += simDesc;
					cnt++;
				}
				
				for (String d1 : MeshLin.docDesc.get(doc)) {
					simDesc = 0.;
					for (String d2 : MeshLin.docDesc.get(d)) {
						tmpVal = 0;
//						if (MeshLin.getCachedDescSim(d2, d1) == null) {
							if (MeshLin.meshDesc.containsKey(d1)) {
								for (Term t1 : MeshLin.meshDesc.get(d1)) {
									if (MeshLin.meshDesc.containsKey(d2)) {
										for (Term t2 : MeshLin.meshDesc.get(d2)) {
											// calc similarity between t1 and t2
											tmpVal = Math.max(tmpVal, t1.compareToLin(t2));
										}
									} else {
										System.err.println("Not in MeSH tree: " + d2);
									}
								}
							} else {
								System.err.println("Not in MeSH tree: " + d1);
							}
//							MeshLin.setCachedDescSim(d2, d1, simDesc);
//						} else {
//							tmpVal = MeshLin.getCachedDescSim(d2, d1);
//						}
//						System.out.println(d1 + " " +  resMesh.getString("descriptor") + ": " + tmpVal);
						simDesc = Math.max(simDesc, tmpVal);
					}
					// similarity between two descriptors
					simDoc += simDesc;
					cnt++;
				}
				
				// similarity between two documents
				simDoc /= cnt;
				simDoc *= -1; // fix ordering for Tree
				
				if (simDoc != 0) {
					if (!results.containsKey(simDoc)) {
						results.put(simDoc, new TreeSet<Integer>());
					}
					results.get(simDoc).add(d);
				}
				
			}
			
			int i = 0;
			insertResults: 
				for (double k : results.keySet()) {
					for (int d : results.get(k)) {
	//					System.out.println(doc + " " + d + ": " + k*-1);
						stmtSim.setInt((i * 3) + 1, doc);
						stmtSim.setInt((i * 3) + 2, d);
						stmtSim.setDouble((i * 3) + 3, k*-1);
						i++;
						if (i == 100) {
							break insertResults;
						}
					}
				}
			stmtSim.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
