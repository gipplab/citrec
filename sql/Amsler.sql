-- Normalize Amsler measures and store to new table

INSERT INTO sim_amsler_rel
SELECT document1, document2, value/(
(SELECT COUNT(1) FROM reference r1o WHERE document=a.document1) +
(SELECT COUNT(1) FROM document d1i LEFT JOIN refdoc_id ri1i ON ri1i.auxId=CAST(d1i.pmId AS CHAR) AND ri1i.type='pm' LEFT JOIN reference r1i ON r1i.refDoc=ri1i.docId WHERE d1i.pmcId=a.document1) +
(SELECT COUNT(1) FROM reference r2o LEFT JOIN refdoc_id ri2o ON ri2o.docId=r2o.refDoc AND ri2o.type='pm' WHERE document=a.document2 
  AND NOT EXISTS (SELECT 1 FROM reference r1o WHERE document=a.document1 AND r1o.refDoc=r2o.refDoc)
  AND NOT EXISTS (SELECT 1 FROM document d1i LEFT JOIN refdoc_id ri1i ON ri1i.auxId=CAST(d1i.pmId AS CHAR) AND ri1i.type='pm' LEFT JOIN reference r1i ON r1i.refDoc=ri1i.docId WHERE d1i.pmcId=a.document1 AND ri2o.auxId=CAST(d1i.pmId AS CHAR))
) +
(SELECT COUNT(1) FROM document d2i LEFT JOIN refdoc_id ri2i ON ri2i.auxId=CAST(d2i.pmId AS CHAR) AND ri2i.type='pm' LEFT JOIN reference r2i ON r2i.refDoc=ri2i.docId WHERE d2i.pmcId=a.document2
  AND NOT EXISTS (SELECT 1 FROM reference r1o LEFT JOIN refdoc_id ri1o ON ri1o.docId=r1o.refDoc AND ri1o.type='pm' LEFT JOIN document d1o ON d1o.pmId=ri1o.auxId WHERE r1o.document=a.document1 AND d1o.pmcId IS NOT NULL AND r2i.document=d1o.pmcId)
  AND NOT EXISTS (SELECT 1 FROM document d1i LEFT JOIN refdoc_id ri1i ON ri1i.auxId=CAST(d1i.pmId AS CHAR) AND ri1i.type='pm' LEFT JOIN reference r1i ON r1i.refDoc=ri1i.docId WHERE d1i.pmcId=a.document1 AND r1i.document=r2i.document)
)) AS value
FROM similarity_amsler a
