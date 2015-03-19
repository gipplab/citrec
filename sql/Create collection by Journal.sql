SET @collection = 'ehp';
SET @method = 'cpa_simple';

SET @s1 = CONCAT('CREATE TABLE IF NOT EXISTS sim_', @collection, '_', @method, ' (
  `document1` int(11) NOT NULL,
  `document2` int(11) NOT NULL,
  `value` double NOT NULL,
  KEY `document1` (`document1`),
  KEY `document2` (`document2`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8');

SET @s2 = CONCAT('ALTER TABLE sim_', @collection, '_', @method, ' DISABLE KEYS');

SET @s3 = CONCAT('INSERT INTO sim_', @collection, '_', @method, ' SELECT * FROM sim_', @method, ' WHERE document1 IN (SELECT id FROM testdocs_', @collection, ') AND document2 IN (SELECT id FROM  testdocs_', @collection, ')');

SET @s4 = CONCAT('ALTER TABLE sim_', @collection, '_', @method, ' ENABLE KEYS');

PREPARE stmt1 FROM @s1;
EXECUTE stmt1;
PREPARE stmt2 FROM @s2;
EXECUTE stmt2;
PREPARE stmt3 FROM @s3;
EXECUTE stmt3;
PREPARE stmt4 FROM @s4;
EXECUTE stmt4;
