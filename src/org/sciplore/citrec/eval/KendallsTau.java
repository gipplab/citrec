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
import java.sql.SQLException;
import java.util.Properties;

import org.sciplore.citrec.Helper;
/**
 * Evaluation utility that calculates the Kendall's tau rank correlation coefficient
 * for two set of documents ranked according to two selected similarity measures.
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 *
 */

public class KendallsTau {
	private final static int NUM_THREADS = 8;
	/**
	 * 
	 *
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException, IOException {
		Properties p = Helper.getProperties();
		
		Class.forName("com.mysql.jdbc.Driver");
		Connection dbs[] = new Connection[NUM_THREADS];
		
		String collections[] = { "" };
		byte selections[] = { 0 }; // , 5, 10, 20
		String methods[] = { "cpa_2simple_tree", "cpa_5simple_tree", "cpa_10simple_tree", "cpa_pow1", "cpa_pow2", "cpa_rt2", "cpa_rt5", "cpa_rt10", "cpa_simple", "cpa_simple_2tree", "cpa_simple_5tree", "cpa_simple_10tree", "cpa_simple_tree", "cpa_sum", "cpa_tree" };
//		String methods[] = { "amsler", "amsler_rel", "bibco", "bibco_rel", "cocit", "cocit_relative", "context_avg", "context_pow1", "context_pow2", "context_rt2", "context_rt5", "context_rt10", "context_sum", "cpa_2simple_tree", "cpa_5simple_tree", "cpa_10simple_tree", "cpa_pow1", "cpa_pow2", "cpa_rt2", "cpa_rt5", "cpa_rt10", "cpa_simple", "cpa_simple_2tree", "cpa_simple_5tree", "cpa_simple_10tree", "cpa_simple_tree", "cpa_sum", "cpa_tree", "linkthrough", "luceneb_title_abstract", "luceneb_title_abstract_text", "lucene_abstract", "lucene_text", "lucene_title"};
		byte initK = 20;
		byte k = initK;
		byte i;
		byte j = 0;
		int cnt = 0;

		KendallsTauThread t[] = new KendallsTauThread[NUM_THREADS];
    	
		for (i = 0; i < NUM_THREADS; i++) {
			dbs[i] = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		}

		i = 0;
		for (byte s : selections) {
			for (String c : collections) {
				if (s == 0) {
					k = initK;
				} else {
					k = s;
				}
				while (k > 0) {
					for (String m : methods) {
						if (++cnt % 100 == 0) {
							System.err.println(cnt);
					    	System.gc();
						}
						
						while (true) {
							if (i == NUM_THREADS) {
				    			i = 0;
				    		}
	
				    		if (t[i] == null || !t[i].isAlive()) {
				    			t[i] = null;
				    			t[i] = new KendallsTauThread(c, m, k, s, dbs[i]);
				    			t[i].start();
				    			i++;
				    			break;
				    		}
				    		if (j == NUM_THREADS) {
						    	try {
									Thread.sleep(100);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								j = 0;
				    		}
	
				    		i++;
				    		j++;
				    	}
					}
					k--;
				}
			}
		}
		
		for (Thread thread : t) {
			thread.join();
		}
		
		for (Connection d : dbs) {
			d.close();
		}		
	}
}
