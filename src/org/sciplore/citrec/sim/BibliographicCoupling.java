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

/**
 * Calculate similarities using Bibliographic Coupling
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class BibliographicCoupling {
	/**
	 * Calculate similarities using Bibliographic Coupling
	 *
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		Logger logger = LoggerFactory.getLogger(BibliographicCoupling.class);
		Properties p = Helper.getProperties();

		
		// initialize database connections begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connections end
				
		Statement stmt = db.createStatement();
		// SQL statement for calculating all couplings for one document
		PreparedStatement stmtBibCo = db.prepareStatement("INSERT INTO similarity_bibco " +
				"SELECT r1.document AS d1, r2.document AS d2, COUNT(DISTINCT r2.refDoc) AS strength " +
				"  FROM reference r1 " +
				"LEFT JOIN reference r2 " +
				"  ON r2.refDoc = r1.refDoc " +
				"WHERE r1.document = ? " +
				"  AND r1.document != r2.document " +
				"  AND r2.refDoc IS NOT NULL " +
				"  GROUP BY r2.document");
		
		stmt.execute("DROP TABLE IF EXISTS sim_bibco");
		stmt.execute("CREATE TABLE IF NOT EXISTS `sim_bibco` (" +
				"`document1` int(11) NOT NULL," +
				"`document2` int(11) NOT NULL," +
				"`value` double NOT NULL," +
				"KEY `document1` (`document1`)," +
				"KEY `document2` (`document2`)" +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
		stmt.execute("ALTER TABLE `sim_bibco` DISABLE KEYS");
		ResultSet resDoc = stmt.executeQuery("SELECT DISTINCT pmcId FROM document");
		
		int cnt = 0;
		
	    while(resDoc.next()) { // iterate over all documents
	    	if(++cnt % 1000 == 0) {
	    		logger.info("{} documents processed.", cnt);
	    	}
			stmtBibCo.setInt(1, resDoc.getInt("pmcId"));
			stmtBibCo.executeUpdate(); // Calculate all couplings for one document and store to database
	    }
	    resDoc.close();
		stmt.execute("ALTER TABLE `sim_bibco` ENABLE KEYS");
		stmt.close();
		stmtBibCo.close();
		db.close();
	}
}
