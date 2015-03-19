SELECT STRAIGHT_JOIN d1.pmcId, d2.pmcId, COUNT(DISTINCT r2.document) AS strength
FROM document d1 
LEFT JOIN refdoc_id ri1 
  ON ri1.type='pm' 
  AND ri1.auxId=d1.pmId 
LEFT JOIN reference r1 
  ON r1.refDoc = ri1.docId 
LEFT JOIN reference r2 
  ON r1.document=r2.document 
  AND r1.refDoc != r2.refDoc 
LEFT JOIN refdoc_id ri2 
  ON ri2.docId=r2.refDoc 
  AND ri2.type='pm' 
LEFT JOIN document d2 
  ON d2.pmId=ri2.auxId 
WHERE d1.pmcId = ? 
  AND d2.pmcId IS NOT NULL 
GROUP BY r2.refDoc
ORDER BY strength DESC

-- Normalize
INSERT INTO similarity_cocit_relative 
SELECT document1, document2, value/(
(SELECT COUNT(r1.document) 
FROM document d1 
LEFT JOIN refdoc_id ri1 ON ri1.auxId=CAST(d1.pmId AS CHAR) AND ri1.type='pm' 
LEFT JOIN reference r1 ON r1.refDoc=ri1.docId WHERE d1.pmcId=c.document1) + 
(SELECT COUNT(r2.document) 
FROM document d2 
LEFT JOIN refdoc_id ri2 ON ri2.auxId=CAST(d2.pmId AS CHAR) AND ri2.type='pm' 
LEFT JOIN reference r2 ON r2.refDoc=ri2.docId 
WHERE d2.pmcId=c.document2) - value) 
AS value 
FROM similarity_cocit c;
