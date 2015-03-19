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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.lucene.store.NIOFSDirectory;
import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Calculate similarities for Lucene MoreLikeThis
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class LuceneMoreLikeThis {
	private static Logger logger = LoggerFactory.getLogger(LuceneMoreLikeThis.class);

	/**
	 * Calculate similarities for Lucene MoreLikeThis
	 *
	 * @param args fields to use for similarity calculations
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Properties p = Helper.getProperties();
		
		if (args.length == 0) {
			System.err.println("Usage: <programname> <title,abstract,text>...");
			System.exit(-1);
		}
		
		for (String a : args) {
			if (!a.equals("title") && !a.equals("abstract") && !a.equals("text")) {
				System.err.println("Usage: <programname> <title,abstract,text>...");
				System.exit(-1);
			}
		}

		File rootDir = new File(p.getProperty("rootDir", "."));
		if (!rootDir.isDirectory()) {
			throw new Exception("rootDir is not a valid directory.");
		}
		
		File indexDir = new File(p.getProperty("indexDir", rootDir + File.separator + "index"));
		if (!indexDir.isDirectory() && !indexDir.mkdir()) {
			throw new Exception("Could not find index directory.");
		}

		// initialize database connections begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connections end

		IndexReader in = IndexReader.open(new NIOFSDirectory(indexDir));
		IndexSearcher is = new IndexSearcher(in);

		Statement stmt = db.createStatement();
		stmt.execute("DROP TABLE IF EXISTS sim_lucene" + Joiner.on('_').join(args));
		stmt.execute("CREATE TABLE IF NOT EXISTS `sim_lucene_" + Joiner.on('_').join(args) + "` (" +
				"`document1` int(11) NOT NULL," +
				"`document2` int(11) NOT NULL," +
				"`value` double NOT NULL," +
				"KEY `document1` (`document1`)," +
				"KEY `document2` (`document2`)" +
				") ENGINE=MyISAM DEFAULT CHARSET=utf8;");
		stmt.execute("ALTER TABLE `sim_lucene_" + Joiner.on('_').join(args) + "` DISABLE KEYS");
		
		PreparedStatement stmtsim = db.prepareStatement("INSERT INTO `sim_lucene_" + Joiner.on('_').join(args) + "` VALUES(?, ?, ?)");

		Set<String> stopwords = new HashSet<String>();
		stopwords.add("a");
		stopwords.add("about");
		stopwords.add("again");
		stopwords.add("all");
		stopwords.add("almost");
		stopwords.add("also");
		stopwords.add("although");
		stopwords.add("always");
		stopwords.add("among");
		stopwords.add("an");
		stopwords.add("and");
		stopwords.add("another");
		stopwords.add("any");
		stopwords.add("are");
		stopwords.add("as");
		stopwords.add("at");
		stopwords.add("be");
		stopwords.add("because");
		stopwords.add("been");
		stopwords.add("before");
		stopwords.add("being");
		stopwords.add("between");
		stopwords.add("both");
		stopwords.add("but");
		stopwords.add("by");
		stopwords.add("can");
		stopwords.add("could");
		stopwords.add("did");
		stopwords.add("do");
		stopwords.add("does");
		stopwords.add("done");
		stopwords.add("due");
		stopwords.add("during");
		stopwords.add("each");
		stopwords.add("either");
		stopwords.add("enough");
		stopwords.add("especially");
		stopwords.add("etc");
		stopwords.add("for");
		stopwords.add("found");
		stopwords.add("from");
		stopwords.add("further");
		stopwords.add("had");
		stopwords.add("has");
		stopwords.add("have");
		stopwords.add("having");
		stopwords.add("here");
		stopwords.add("how");
		stopwords.add("however");
		stopwords.add("i");
		stopwords.add("if");
		stopwords.add("in");
		stopwords.add("into");
		stopwords.add("is");
		stopwords.add("it");
		stopwords.add("its");
		stopwords.add("itself");
		stopwords.add("just");
		stopwords.add("kg");
		stopwords.add("km");
		stopwords.add("made");
		stopwords.add("mainly");
		stopwords.add("make");
		stopwords.add("may");
		stopwords.add("mg");
		stopwords.add("might");
		stopwords.add("ml");
		stopwords.add("mm");
		stopwords.add("most");
		stopwords.add("mostly");
		stopwords.add("must");
		stopwords.add("nearly");
		stopwords.add("neither");
		stopwords.add("no");
		stopwords.add("nor");
		stopwords.add("obtained");
		stopwords.add("of");
		stopwords.add("often");
		stopwords.add("on");
		stopwords.add("our");
		stopwords.add("overall");
		stopwords.add("perhaps");
		stopwords.add("pmid");
		stopwords.add("quite");
		stopwords.add("rather");
		stopwords.add("really");
		stopwords.add("regarding");
		stopwords.add("seem");
		stopwords.add("seen");
		stopwords.add("several");
		stopwords.add("should");
		stopwords.add("show");
		stopwords.add("showed");
		stopwords.add("shown");
		stopwords.add("shows");
		stopwords.add("significantly");
		stopwords.add("since");
		stopwords.add("so");
		stopwords.add("some");
		stopwords.add("such");
		stopwords.add("than");
		stopwords.add("that");
		stopwords.add("the");
		stopwords.add("their");
		stopwords.add("theirs");
		stopwords.add("them");
		stopwords.add("then");
		stopwords.add("there");
		stopwords.add("therefore");
		stopwords.add("these");
		stopwords.add("they");
		stopwords.add("this");
		stopwords.add("those");
		stopwords.add("through");
		stopwords.add("thus");
		stopwords.add("to");
		stopwords.add("upon");
		stopwords.add("use");
		stopwords.add("used");
		stopwords.add("using");
		stopwords.add("various");
		stopwords.add("very");
		stopwords.add("was");
		stopwords.add("we");
		stopwords.add("were");
		stopwords.add("what");
		stopwords.add("when");
		stopwords.add("which");
		stopwords.add("while");
		stopwords.add("with");
		stopwords.add("within");
		stopwords.add("without");
		stopwords.add("would");
		
		MoreLikeThis mlt = new MoreLikeThis(in);
		mlt.setBoost(true);
		mlt.setFieldNames(args);
		mlt.setMinWordLen(2);
		mlt.setStopWords(stopwords);

		for(int i=0; i<in.maxDoc(); i++) {
			Document doc = in.document(i);
			if(i % 1000 == 0) {
				logger.info("{} documents processed.", i);
			}
			Query query = mlt.like(i);
			TopDocs docs = is.search(query, 101);
			
			for(ScoreDoc d : docs.scoreDocs) {
				if (Integer.parseInt(doc.get("pmcId")) != Integer.parseInt(in.document(d.doc).get("pmcId"))) {
		    		stmtsim.setInt(1, Integer.parseInt(doc.get("pmcId")));
		    		stmtsim.setInt(2, Integer.parseInt(in.document(d.doc).get("pmcId")));
		    		stmtsim.setDouble(3, d.score);
		    		stmtsim.executeUpdate();
				}
			}
		}
		stmtsim.close();
		is.close();
		in.close();

		stmt.execute("ALTER TABLE `sim_lucene_" + Joiner.on('_').join(args) + "` ENABLE KEYS");
		stmt.close();
		db.close();
	}
}