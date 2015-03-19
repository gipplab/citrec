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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.sciplore.citrec.Helper;

/**
 * Calculates the Information Content of descriptors in the MeSH thesaurus.
 * 
 * The Information Content is calculated while the data is read from the database.
 * The Order ensures that required values are already computed when needed.
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class MeshInformationContent {
	public static HashMap<String, HashMap<Short, Object>> mTree = new HashMap<String, HashMap<Short, Object>>();
	// field desciptors for recursive data structure used.
	private final static short VALUE = 0;
	private final static short CHILDREN = 1;
	private final static short DESCRIPTOR = 2;
	private final static short ID = 3;
	private static int N;
	private static PreparedStatement stmtIc;
	private static HashMap<String, HashMap<Short, Object>> tmp;
	
	/**
	 * Calculate the Information Content in the MeSH tree.
	 *
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Properties p = Helper.getProperties();
		
		// initialize database connection begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connection end		

		stmtIc = db.prepareStatement("UPDATE meshtree SET ic=? WHERE id=?");
		ResultSet resN = db.createStatement().executeQuery("SELECT COUNT(1) AS cnt FROM meshtree");
		
		resN.next();
		N = resN.getInt("cnt");
			 
		Statement stmtdoc = db.createStatement();
		ResultSet resMesh = stmtdoc.executeQuery("SELECT id, descriptor FROM meshtree ORDER BY id DESC");
		
		List<String> ids;
		int i = 0;
		
		// iterate through all entries in the MeSH tree
		while (resMesh.next()) {
			if (++i % 100 == 0) {
				System.out.println(i);
			}
			ids = new LinkedList<String>();
			for (String id : resMesh.getString("id").split("\\.")) {
				ids.add(id);
			}
			add(mTree, ids, resMesh.getString("descriptor"), resMesh.getString("id"));
		}
	}
	
	/**
	 * Add an entry to the recursive data structure for the MeSH tree.
	 *
	 * @param tree
	 * @param ids
	 * @param descriptor
	 * @param id
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private static void add(HashMap<String, HashMap<Short, Object>> tree, List<String> ids, String descriptor, String id) throws SQLException {
		if (!tree.containsKey(ids.get(0))) {
			tree.put(ids.get(0), new HashMap<Short, Object>());
		}
		if (ids.size() > 1) {
			if (!tree.get(ids.get(0)).containsKey(CHILDREN)) {
				tree.get(ids.get(0)).put(CHILDREN, new HashMap<String, HashMap<Short, Object>>());
			}
			String tmp = ids.remove(0);
			add((HashMap<String, HashMap<Short, Object>>)tree.get(tmp).get(CHILDREN), ids, descriptor, id);
		} else {
			tree.get(ids.get(0)).put(DESCRIPTOR, descriptor);
			tree.get(ids.get(0)).put(ID, id);
			double p = (double)1/(double)N;
			if (tree.get(ids.get(0)).containsKey(CHILDREN)) {
				tmp = ((HashMap<String, HashMap<Short, Object>>)tree.get(ids.get(0)).get(CHILDREN));
				for (String k : tmp.keySet()) {
					p += (Double)tmp.get(k).get(VALUE);
				}
			}
			tree.get(ids.get(0)).put(VALUE, p);
			// set information content in database for just added entry
			stmtIc.setDouble(1, Math.log(p)*-1);
			stmtIc.setString(2, id);
			stmtIc.executeUpdate();
		}
	}

//	private static void print(HashMap<String, HashMap<Short, Object>> tree) {
//		for (String k : tree.keySet()) {
//			System.out.println(((HashMap<Short, Object>)tree.get(k)).get(ID) + "\t" + ((HashMap<Short, Object>)tree.get(k)).get(DESCRIPTOR) + "\t" + Math.log((Double)((HashMap<Short, Object>)tree.get(k)).get(VALUE))*-1);
//			if ((((HashMap<Short, Object>)tree.get(k)).containsKey(CHILDREN))) {
//				print(((HashMap<String, HashMap<Short, Object>>)tree.get(k).get(CHILDREN)));
//			}
//		}
//	}
}
