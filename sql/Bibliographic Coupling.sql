INSERT INTO similarity_bibco 
  SELECT r1.document AS d1, r2.document AS d2, COUNT(DISTINCT r2.refDoc) AS strength 
  FROM reference r1 
  LEFT JOIN reference r2 
  	ON r2.refDoc = r1.refDoc 
  WHERE r1.document = ?
    AND r1.document != r2.document 
    AND r2.refDoc IS NOT NULL
  GROUP BY r2.document
  ORDER BY strength DESC
  
-- Normalize
INSERT INTO similarity_bibco_rel 
SELECT document1, document2, value / (
(SELECT COUNT(1) FROM reference WHERE document=b.document1) + 
(SELECT COUNT(1) FROM reference WHERE document=b.document2) - value) 
AS value 
FROM similarity_bibco b;
