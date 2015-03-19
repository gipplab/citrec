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
import java.util.Map;
import java.util.Properties;

import org.sciplore.citrec.Helper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Aggregates referenced documents so they can be identified.
 *
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 */
public class ReferenceDocumentsDisambiguator {
	private static Logger logger = LoggerFactory.getLogger(ReferenceDocumentsDisambiguator.class);

	/**
	 * Main program for aggregating references.
	 *
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		logger.info("Initializing.");
		Properties p = Helper.getProperties();

		// initialize database connections begin
		Class.forName(p.getProperty("db.driver"));
		Connection db = DriverManager.getConnection(p.getProperty("db.url"), p.getProperty("db.user"), p.getProperty("db.password"));
		// initialize database connections end

		Statement stmtDoc = db.createStatement();
		Statement stmt = db.createStatement();
		
//		PreparedStatement stmtDocId = db.prepareStatement("SELECT docId FROM refdoc_id WHERE type=? AND auxId=?");
		PreparedStatement stmtAuxIds = db.prepareStatement("SELECT type, auxId FROM refdoc_id WHERE docId=?");
		PreparedStatement stmtAddRefId = db.prepareStatement("INSERT INTO refdoc_id (docId, type, auxId) VALUES(?, ?, ?)");
		PreparedStatement stmtSetRefDoc = db.prepareStatement("UPDATE reference SET refDoc=? WHERE id=?");
		PreparedStatement stmtMakeNewRefDocId = db.prepareStatement("INSERT INTO refdoc_seq VALUES(default)");
		PreparedStatement stmtGetNewRefDocId = db.prepareStatement("SELECT LAST_INSERT_ID()");
		
		int i = 0;
		int docId = 0;
		
		logger.info("Getting existing IDs.");
		
		Map<String, Integer> pmIds = new HashMap<String, Integer>();
		Map<String, Integer> pmcIds = new HashMap<String, Integer>();
		Map<String, Integer> medIds = new HashMap<String, Integer>();
		Map<String, Integer> dois = new HashMap<String, Integer>();
		Multimap<String, Integer> authKeys = HashMultimap.create();
		Multimap<String, Integer> titKeys = HashMultimap.create();
		
		ResultSet refIds = stmt.executeQuery("SELECT docId, type, auxId FROM refdoc_id");
		String type;
		while (refIds.next()) {
			type = refIds.getString("type");
			if (type.equals("pm")) {
				pmIds.put(refIds.getString("auxId"), refIds.getInt("docId"));
			} else if (type.equals("pmc")) {
				pmcIds.put(refIds.getString("auxId"), refIds.getInt("docId"));
			} else if (type.equals("medline")) {
				medIds.put(refIds.getString("auxId"), refIds.getInt("docId"));
			} else if (type.equals("doi")) {
				dois.put(refIds.getString("auxId"), refIds.getInt("docId"));
			} else if (type.equals("author")) {
				authKeys.put(refIds.getString("auxId"), refIds.getInt("docId"));
			} else if (type.equals("title")) {
				titKeys.put(refIds.getString("auxId"), refIds.getInt("docId"));
			}
		}
		
		stmt.execute("ALTER TABLE `refdoc_id` DISABLE KEYS");
		
		logger.info("Matching by PubMed ID.");
		ResultSet resRefPmId = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND refPmId IS NOT NULL");
		while (resRefPmId.next()) {
			if (++i % 1000 == 0) {
				logger.info("PubMed ID: {}", i);
			}
			
			if (pmIds.containsKey(resRefPmId.getString("refPmId"))) {
				docId = pmIds.get(resRefPmId.getString("refPmId"));
			} else {
				stmtMakeNewRefDocId.executeUpdate();
				ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
				resGetNewRefDocId.next();
				docId = resGetNewRefDocId.getInt(1);
				resGetNewRefDocId.close();
			}
			
			stmtAddRefId.setInt(1, docId);

			if (resRefPmId.getString("refPmId") != null && !pmIds.containsKey(resRefPmId.getString("refPmId"))) {
				stmtAddRefId.setString(2, "pm");
				stmtAddRefId.setString(3, resRefPmId.getString("refPmId"));
				stmtAddRefId.executeUpdate();
				pmIds.put(resRefPmId.getString("refPmId"), docId);
			}
						
			if (resRefPmId.getString("refPmcId") != null && !pmcIds.containsKey(resRefPmId.getString("refPmcId"))) {
				stmtAddRefId.setString(2, "pmc");
				stmtAddRefId.setString(3, resRefPmId.getString("refPmcId"));
				stmtAddRefId.executeUpdate();
				pmcIds.put(resRefPmId.getString("refPmcId"), docId);
			}
			
			if (resRefPmId.getString("refDoi") != null && !dois.containsKey(resRefPmId.getString("refDoi"))) {
				stmtAddRefId.setString(2, "doi");
				stmtAddRefId.setString(3, resRefPmId.getString("refDoi"));
				stmtAddRefId.executeUpdate();
				dois.put(resRefPmId.getString("refDoi"), docId);
			}
			
			if (resRefPmId.getString("refMedId") != null && !medIds.containsKey(resRefPmId.getString("refMedId"))) {
				stmtAddRefId.setString(2, "medline");
				stmtAddRefId.setString(3, resRefPmId.getString("refMedId"));
				stmtAddRefId.executeUpdate();
				pmIds.put(resRefPmId.getString("refMedId"), docId);
			}
			
			if (resRefPmId.getString("refAuthKey") != null && !authKeys.containsEntry(resRefPmId.getString("refAuthKey"), docId)) {
				stmtAddRefId.setString(2, "author");
				stmtAddRefId.setString(3, resRefPmId.getString("refAuthKey"));
				stmtAddRefId.executeUpdate();
				authKeys.put(resRefPmId.getString("refAuthKey"), docId);
			}
			
			if (resRefPmId.getString("refTitKey") != null && !titKeys.containsEntry(resRefPmId.getString("refTitKey"), docId)) {
				stmtAddRefId.setString(2, "title");
				stmtAddRefId.setString(3, resRefPmId.getString("refTitKey"));
				stmtAddRefId.executeUpdate();
				titKeys.put(resRefPmId.getString("refTitKey"), docId);
			}
			
			stmtSetRefDoc.setInt(1, docId);
			stmtSetRefDoc.setInt(2, resRefPmId.getInt("id"));
			stmtSetRefDoc.executeUpdate();
		}
		resRefPmId.close();
		logger.info("Doc ID after PM: {}", docId);

		logger.info("Matching by PubMed Central ID.");
		ResultSet resRefPmcId = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND refPmcId IS NOT NULL");
		while (resRefPmcId.next()) {
			if (++i % 1000 == 0) {
				logger.info("PubMed Central ID: {}", i);
			}
			
			if (pmcIds.containsKey(resRefPmcId.getString("refPmcId"))) {
				docId = pmcIds.get(resRefPmcId.getString("refPmcId"));
			} else {
				stmtMakeNewRefDocId.executeUpdate();
				ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
				resGetNewRefDocId.next();
				docId = resGetNewRefDocId.getInt(1);
				resGetNewRefDocId.close();
			}
			
			stmtAddRefId.setInt(1, docId);

			if (resRefPmcId.getString("refPmcId") != null && !pmcIds.containsKey(resRefPmcId.getString("refPmcId"))) {
				stmtAddRefId.setString(2, "pmc");
				stmtAddRefId.setString(3, resRefPmcId.getString("refPmcId"));
				stmtAddRefId.executeUpdate();
				pmcIds.put(resRefPmcId.getString("refPmcId"), docId);
			}
			
			if (resRefPmcId.getString("refDoi") != null && !dois.containsKey(resRefPmcId.getString("refDoi"))) {
				stmtAddRefId.setString(2, "doi");
				stmtAddRefId.setString(3, resRefPmcId.getString("refDoi"));
				stmtAddRefId.executeUpdate();
				dois.put(resRefPmcId.getString("refDoi"), docId);
			}
			
			if (resRefPmcId.getString("refMedId") != null && !medIds.containsKey(resRefPmcId.getString("refMedId"))) {
				stmtAddRefId.setString(2, "medline");
				stmtAddRefId.setString(3, resRefPmcId.getString("refMedId"));
				stmtAddRefId.executeUpdate();
				pmIds.put(resRefPmcId.getString("refMedId"), docId);
			}
			
			if (resRefPmcId.getString("refAuthKey") != null && !authKeys.containsEntry(resRefPmcId.getString("refAuthKey"), docId)) {
				stmtAddRefId.setString(2, "author");
				stmtAddRefId.setString(3, resRefPmcId.getString("refAuthKey"));
				stmtAddRefId.executeUpdate();
				authKeys.put(resRefPmcId.getString("refAuthKey"), docId);
			}
			
			if (resRefPmcId.getString("refTitKey") != null && !titKeys.containsEntry(resRefPmcId.getString("refTitKey"), docId)) {
				stmtAddRefId.setString(2, "title");
				stmtAddRefId.setString(3, resRefPmcId.getString("refTitKey"));
				stmtAddRefId.executeUpdate();
				titKeys.put(resRefPmcId.getString("refTitKey"), docId);
			}
			
			stmtSetRefDoc.setInt(1, docId);
			stmtSetRefDoc.setInt(2, resRefPmcId.getInt("id"));
			stmtSetRefDoc.executeUpdate();
		}
		resRefPmcId.close();
		logger.info("Doc ID after PMC: {}", docId);

		logger.info("Matching by DOI.");
		ResultSet resRefDoi = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND refDoi IS NOT NULL");
		while (resRefDoi.next()) {
			if (++i % 1000 == 0) {
				logger.info("DOI: {}", i);
			}
			
			if (dois.containsKey(resRefDoi.getString("refDoi"))) {
				docId = dois.get(resRefDoi.getString("refDoi"));
			} else {
				stmtMakeNewRefDocId.executeUpdate();
				ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
				resGetNewRefDocId.next();
				docId = resGetNewRefDocId.getInt(1);
				resGetNewRefDocId.close();
			}
			
			stmtAddRefId.setInt(1, docId);

			if (resRefDoi.getString("refDoi") != null && !dois.containsKey(resRefDoi.getString("refDoi"))) {
				stmtAddRefId.setString(2, "doi");
				stmtAddRefId.setString(3, resRefDoi.getString("refDoi"));
				stmtAddRefId.executeUpdate();
				dois.put(resRefDoi.getString("refDoi"), docId);
			}
			
			if (resRefDoi.getString("refMedId") != null && !medIds.containsKey(resRefDoi.getString("refMedId"))) {
				stmtAddRefId.setString(2, "medline");
				stmtAddRefId.setString(3, resRefDoi.getString("refMedId"));
				stmtAddRefId.executeUpdate();
				pmIds.put(resRefDoi.getString("refMedId"), docId);
			}
			
			if (resRefDoi.getString("refAuthKey") != null && !authKeys.containsEntry(resRefDoi.getString("refAuthKey"), docId)) {
				stmtAddRefId.setString(2, "author");
				stmtAddRefId.setString(3, resRefDoi.getString("refAuthKey"));
				stmtAddRefId.executeUpdate();
				authKeys.put(resRefDoi.getString("refAuthKey"), docId);
			}
			
			if (resRefDoi.getString("refTitKey") != null && !titKeys.containsEntry(resRefDoi.getString("refTitKey"), docId)) {
				stmtAddRefId.setString(2, "title");
				stmtAddRefId.setString(3, resRefDoi.getString("refTitKey"));
				stmtAddRefId.executeUpdate();
				titKeys.put(resRefDoi.getString("refTitKey"), docId);
			}
			
			stmtSetRefDoc.setInt(1, docId);
			stmtSetRefDoc.setInt(2, resRefDoi.getInt("id"));
			stmtSetRefDoc.executeUpdate();
		}
		resRefDoi.close();
		logger.info("Doc ID after DOI: {}", docId);

		logger.info("Matching by Medline ID.");
		ResultSet resRefMedId = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND refMedId IS NOT NULL");
		while (resRefMedId.next()) {
			if (++i % 1000 == 0) {
				logger.info("Medline ID: {}", i);
			}
			
			if (medIds.containsKey(resRefMedId.getString("refMedId"))) {
				docId = medIds.get(resRefMedId.getString("refMedId"));
			} else {
				stmtMakeNewRefDocId.executeUpdate();
				ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
				resGetNewRefDocId.next();
				docId = resGetNewRefDocId.getInt(1);
				resGetNewRefDocId.close();
			}
			
			stmtAddRefId.setInt(1, docId);

			if (resRefMedId.getString("refMedId") != null && !medIds.containsKey(resRefMedId.getString("refMedId"))) {
				stmtAddRefId.setString(2, "medline");
				stmtAddRefId.setString(3, resRefMedId.getString("refMedId"));
				stmtAddRefId.executeUpdate();
				pmIds.put(resRefMedId.getString("refMedId"), docId);
			}
			
			if (resRefMedId.getString("refAuthKey") != null && !authKeys.containsEntry(resRefMedId.getString("refAuthKey"), docId)) {
				stmtAddRefId.setString(2, "author");
				stmtAddRefId.setString(3, resRefMedId.getString("refAuthKey"));
				stmtAddRefId.executeUpdate();
				authKeys.put(resRefMedId.getString("refAuthKey"), docId);
			}
			
			if (resRefMedId.getString("refTitKey") != null && !titKeys.containsEntry(resRefMedId.getString("refTitKey"), docId)) {
				stmtAddRefId.setString(2, "title");
				stmtAddRefId.setString(3, resRefMedId.getString("refTitKey"));
				stmtAddRefId.executeUpdate();
				titKeys.put(resRefMedId.getString("refTitKey"), docId);
			}
			
			stmtSetRefDoc.setInt(1, docId);
			stmtSetRefDoc.setInt(2, resRefMedId.getInt("id"));
			stmtSetRefDoc.executeUpdate();
		}
		resRefMedId.close();
		logger.info("Doc ID after Medline: {}", docId);

		logger.info("Matching by author and title.");
		ResultSet resRefAuthorTitle = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND (refAuthKey IS NOT NULL AND refTitKey IS NOT NULL)");
		while (resRefAuthorTitle.next()) {
			if (++i % 1000 == 0) {
				logger.info("Author and title: {}", i);
			}
			
			int id = 0;
			for (Integer td : titKeys.get(resRefAuthorTitle.getString("refTitKey"))) {
				for (Integer ad : authKeys.get(resRefAuthorTitle.getString("refAuthKey"))) {
					if (ad.equals(td)) {
						if (id != 0) {
							logger.warn("Multiple matches for Author: {} and Title: {}", resRefAuthorTitle.getString("refAuthKey"), resRefAuthorTitle.getString("refTitKey"));
						}
						id = td;
					}
				}
			}
			
			if (id != 0) {
				docId = id;
				stmtSetRefDoc.setInt(1, docId);
				stmtSetRefDoc.setInt(2, resRefAuthorTitle.getInt("id"));
				stmtSetRefDoc.executeUpdate();
			}
		}
		resRefAuthorTitle.close();
		logger.info("Doc ID after Author + Title: {}", docId);

//		logger.info("Matching by author and title (fuzzy).");
//		PreparedStatement stmtDocId = db.prepareStatement("SELECT docId FROM (SELECT i1.docId, i1.auxId as tit, i2.auxId as auth FROM refdoc_id i1 INNER JOIN refdoc_id i2 ON i2.type='author' AND i2.auxId LIKE ? AND LENGTH(i2.auxId) <= ? AND LENGTH(i2.auxId) >= ? AND i1.docId=i2.docId WHERE i1.type='title' AND i1.auxId LIKE ? AND LENGTH(i1.auxId) <= ? AND LENGTH(i1.auxId) >= ?) A WHERE levenshtein_ratio(A.auth, ?) > 90 AND levenshtein_ratio(A.tit, ?) > 90 ORDER BY (levenshtein_ratio(A.auth, ?) + levenshtein_ratio(A.tit, ?)) DESC");
//		ResultSet resRefAuthorTitle2 = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL AND (refAuthKey IS NOT NULL AND refTitKey IS NOT NULL)");
//		while (resRefAuthorTitle2.next()) {
//			if (++i % 1 == 0) {
//				logger.info("Author and title (fuzzy): {}", i);
//			}
//			
//			String tit = resRefAuthorTitle2.getString("refTitKey");
//			String aut = resRefAuthorTitle2.getString("refAuthKey");
//			stmtDocId.setString(1, aut.substring(0, 1) + "%");
//			stmtDocId.setInt(2, aut.length() + (int)Math.ceil(aut.length() * .1));
//			stmtDocId.setInt(3, aut.length() - (int)Math.ceil(aut.length() * .1));
//			stmtDocId.setString(4, tit.substring(0, 1) + "%");
//			stmtDocId.setInt(5, tit.length() + (int)Math.ceil(tit.length() * .1));
//			stmtDocId.setInt(6, tit.length() - (int)Math.ceil(tit.length() * .1));
//			stmtDocId.setString(7, aut);
//			stmtDocId.setString(8, tit);
//			stmtDocId.setString(9, aut);
//			stmtDocId.setString(10, tit);
//			ResultSet resDocId = stmtDocId.executeQuery();
//			
//			if (resDocId.next()) {
//				docId = resDocId.getInt("docId");
//				logger.debug("Found mathching document {} for Author: {} and Title: {}.", new Object[] { docId, aut, tit });
//			} else {
//				stmtMakeNewRefDocId.executeUpdate();
//				ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
//				resGetNewRefDocId.next();
//				docId = resGetNewRefDocId.getInt(1);
//				resGetNewRefDocId.close();
//			}
//			
//			stmtAddRefId.setInt(1, docId);
//
//			if (resRefAuthorTitle2.getString("refAuthKey") != null && !authKeys.containsEntry(resRefAuthorTitle2.getString("refAuthKey"), docId)) {
//				stmtAddRefId.setString(2, "author");
//				stmtAddRefId.setString(3, resRefAuthorTitle2.getString("refAuthKey"));
//				stmtAddRefId.executeUpdate();
//				authKeys.put(resRefAuthorTitle2.getString("refAuthKey"), docId);
//			}
//			
//			if (resRefAuthorTitle2.getString("refTitKey") != null && !titKeys.containsEntry(resRefAuthorTitle2.getString("refTitKey"), docId)) {
//				stmtAddRefId.setString(2, "title");
//				stmtAddRefId.setString(3, resRefAuthorTitle2.getString("refTitKey"));
//				stmtAddRefId.executeUpdate();
//				titKeys.put(resRefAuthorTitle2.getString("refTitKey"), docId);
//			}
//			
//			stmtSetRefDoc.setInt(1, docId);
//			stmtSetRefDoc.setInt(2, resRefAuthorTitle2.getInt("id"));
//			stmtSetRefDoc.executeUpdate();
//			
//			resDocId.close();
//		}
//		resRefAuthorTitle2.close();
//		stmtDocId.close();
//		logger.info("Doc ID after Author + Title (fuzzy): {}", docId);
		
		
		logger.info("New IDs for the rest.");
		ResultSet resRefOther = stmtDoc.executeQuery("SELECT id, refPmId, refPmcId, refMedId, refDoi, refAuthKey, refTitKey FROM reference WHERE refDoc IS NULL");
		while (resRefOther.next()) {
			if (++i % 1000 == 0) {
				logger.info("Rest: {}", i);
			}
			
			stmtMakeNewRefDocId.executeUpdate();
			ResultSet resGetNewRefDocId = stmtGetNewRefDocId.executeQuery();
			resGetNewRefDocId.next();
			docId = resGetNewRefDocId.getInt(1);
			resGetNewRefDocId.close();
			
			stmtAddRefId.setInt(1, docId);

			if (resRefOther.getString("refAuthKey") != null) {
				stmtAddRefId.setString(2, "author");
				stmtAddRefId.setString(3, resRefOther.getString("refAuthKey"));
				stmtAddRefId.executeUpdate();
				authKeys.put(resRefOther.getString("refAuthKey"), docId);
			}
			
			if (resRefOther.getString("refTitKey") != null) {
				stmtAddRefId.setString(2, "title");
				stmtAddRefId.setString(3, resRefOther.getString("refTitKey"));
				stmtAddRefId.executeUpdate();
				titKeys.put(resRefOther.getString("refTitKey"), docId);
			}

			stmtSetRefDoc.setInt(1, docId);
			stmtSetRefDoc.setInt(2, resRefOther.getInt("id"));
			stmtSetRefDoc.executeUpdate();
		}
		resRefOther.close();
		logger.info("Doc ID when finished: {}", docId);
		
		stmtAddRefId.close();
		stmtAuxIds.close();
		stmtDoc.close();
		stmtGetNewRefDocId.close();
		stmtMakeNewRefDocId.close();
		stmtSetRefDoc.close();

		stmt.execute("ALTER TABLE `refdoc_id` ENABLE KEYS");
		stmt.close();

		db.close();
	}
}
