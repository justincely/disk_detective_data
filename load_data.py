import glob
from sqlalchemy import create_engine
from astropy.io import ascii

import yaml

settings = yaml.load('~/dd_configure.yaml')

engine = create_engine(settings['CONNECTION_STRING'], echo=True)
connection = engine.connect()

try:
    connection.execute("""CREATE TABLE sed (designation char(20), ra double, declination double, glon double, glat double, w1mpro double, w1sigmpro double, w2mpro double, w2sigmpro double, w3mpro double, w3sigmpro double, w4mpro double, w4sigmpro double,  j_m_2mass double, j_msig_2mass double, h_m_2mass double, h_msig_2mass double, k_m_2mass double, k_msig_2mass double, PRIMARY KEY (designation) );""")
    connection.execute("""CREATE INDEX name ON designation;""")
except:
    pass


for item in glob.glob('data/WZ_subjects_extflag*.txt'):
    print("parsing {}".format(item))
    data = ascii.read(item)
    for row in data:
        connection.execute("""INSERT INTO sed VALUES {}""".format(row.__array__()))

for item in glob.glob('data/WZ_removethesesubjects_*.txt'):
    print("deleting {}".format(item))
    for name in [line.strip() for line in open(item).readlines() if line.strip().startswith('J')]:
        connection.execute("""DELETE FROM sed WHERE designation='{}'""".format(name))
