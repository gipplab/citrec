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

import java.io.File;
import java.util.Properties;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies the Lucene index and assigns boosts to specified fields.
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class IndexCopier {
	private static Logger logger = LoggerFactory.getLogger(IndexCopier.class);

	/**
	 *  Copy the Lucene index and assign boosts to the fields.
	 *
	 * @param args unused.
	 * @throws Exception on error.
	 */
	public static void main(String[] args) throws Exception {
		Properties p = Helper.getProperties();

		File rootDir = new File(p.getProperty("rootDir", "."));
		if (!rootDir.isDirectory()) {
			throw new Exception("rootDir is not a valid directory.");
		}
		
		File indexDir = new File(p.getProperty("indexDir", rootDir + File.separator + "index"));
		if (!indexDir.isDirectory() && !indexDir.mkdir()) {
			throw new Exception("Could not find index directory.");
		}

		File indexBoostDir = new File(p.getProperty("indexBoostDir", rootDir + File.separator + "index_boost"));
		if (!indexBoostDir.isDirectory() && !indexBoostDir.mkdir()) {
			throw new Exception("Could not find index boost directory.");
		}
		
		int boostTitle = Integer.parseInt(p.getProperty("indexBoostTitle", "4"));
		int boostAbstract = Integer.parseInt(p.getProperty("indexBoostAbstract", "2"));
		int boostText = Integer.parseInt(p.getProperty("indexBoostText", "1"));

		IndexReader in = IndexReader.open(new NIOFSDirectory(indexDir));
		IndexWriter indexWriter = new IndexWriter(new NIOFSDirectory(indexBoostDir), new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34)));

		for(int i=0; i<in.maxDoc(); i++) {
			if (i % 1000 == 0) {
				logger.info("Indexing document {}", i);
			}
			
			Document doc = in.document(i);
			Document luceneDoc = new org.apache.lucene.document.Document();
			luceneDoc.add(new NumericField("pmcId", Store.YES, false).setIntValue(Integer.parseInt(doc.get("pmcId"))));
			luceneDoc.add(new Field("file", doc.get("file"), Store.YES, Index.NO));
			Field title = new Field("title", doc.get("title"), Store.YES, Index.ANALYZED, TermVector.YES);
			title.setBoost(boostTitle);
			luceneDoc.add(title);
			Field abstractField = new Field("abstract", doc.get("abstract"), Store.YES, Index.ANALYZED, TermVector.YES);
			abstractField.setBoost(boostAbstract);
			luceneDoc.add(abstractField);
			Field text = new Field("text", doc.get("text"), Store.YES, Index.ANALYZED, TermVector.YES);
			text.setBoost(boostText);
			luceneDoc.add(text);
			indexWriter.addDocument(luceneDoc);
			luceneDoc = null;
			text = null;
			abstractField = null;
			title = null;
		}
		indexWriter.optimize();
		indexWriter.close();
		in.close();
	}
}