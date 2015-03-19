SET @k = 5;

SET @s1 = CONCAT('CREATE TABLE IF NOT EXISTS testdocs_x_', @k, ' (
  `id` int(11) NOT NULL PRIMARY KEY
) ENGINE=MyISAM DEFAULT CHARSET=utf8');

SET @s2 = CONCAT('INSERT INTO testdocs_x_', @k, ' SELECT document1 FROM sim_bibco GROUP BY document1 HAVING COUNT(1) >= ', @k);

SET @s3 = CONCAT('DELETE FROM testdocs_x_', @k, ' WHERE NOT EXISTS (SELECT 1 FROM sim_mesh_lin WHERE document1=id GROUP BY document1 HAVING COUNT(1) >= ', @k, ')');

SET @s4 = CONCAT('DELETE FROM testdocs_x_', @k, ' WHERE NOT EXISTS (SELECT 1 FROM sim_lucene_abstract WHERE document1=id GROUP BY document1 HAVING COUNT(1) >= ', @k, ')');

SET @s5 = CONCAT('DELETE FROM testdocs_x_', @k, ' WHERE NOT EXISTS (SELECT 1 FROM sim_cocit WHERE document1=id GROUP BY document1 HAVING COUNT(1) >= ', @k, ')');

PREPARE stmt1 FROM @s1;
EXECUTE stmt1;
PREPARE stmt2 FROM @s2;
EXECUTE stmt2;
PREPARE stmt3 FROM @s3;
EXECUTE stmt3;
PREPARE stmt4 FROM @s4;
EXECUTE stmt4;
PREPARE stmt5 FROM @s5;
EXECUTE stmt5;
