/* Test script 
 */

SHOW DATABASES;

USE uvsed;

SHOW TABLES;


CREATE TABLE sed (designation char(20),
                  ra double, 
                  declination double,
                  glon double,
                  glat double,
                  w1mpro double,
                  w1sigmpro double,
                  w2mpro double, 
                  w2sigmpro double, 
                  w3mpro double, 
                  w3sigmpro double,
                  w4mpro double,
                  w4sigmpro double, 
                  j_m_2mass double,
                  j_msig_2mass double,
                  h_m_2mass double,
                  h_msig_2mass double,
                  k_m_2mass double,
                  k_msig_2mass double,
                  PRIMARY KEY (designation) );

SHOW TABLES;

LOAD DATA LOCAL INFILE 'data/WZ_subjects_extflag+0.5to+1.txt' INTO TABLE sed
    FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
