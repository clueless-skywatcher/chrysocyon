CREATE TABLE table1 (INT id1, VARSTR(100) name1);
CREATE TABLE table2 (INT id2, VARSTR(100) name2);

INSERT INTO table1 (id1, name1) VALUES (1, "ABC");
INSERT INTO table1 (id1, name1) VALUES (2, "DEF");

INSERT INTO table2 (id2, name2) VALUES (3, "GHI");
INSERT INTO table2 (id2, name2) VALUES (4, "JKL");

SELECT * FROM table1, table2;