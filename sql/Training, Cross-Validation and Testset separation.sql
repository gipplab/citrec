INSERT INTO doc_test
SELECT DISTINCT d.pmcId
FROM document d
LEFT JOIN doc_train t ON t.id=d.pmcId
LEFT JOIN doc_cv c ON c.id=d.pmcId
WHERE t.id IS NULL AND c.id IS NULL
ORDER BY RAND()