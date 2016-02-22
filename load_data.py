import glob
from sqlalchemy import create_engine
from astropy.io import ascii
import os

import yaml

from db_tables import load_connection, open_settings
from db_tables import Sed
from db_tables import Base

SETTINGS = yaml.load(open(os.path.join(os.environ['HOME'], 'dd_configure.yaml')))
print SETTINGS

Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=True)

#-------------------------------------------------------------------------------

def clean(value):
    if isinstance(value, float):
        return round(value, 7)
    elif isinstance(value, str):
        if value == 'nan':
            return None
        if value == 'masked':
            return None
    if str(value) == '--':
        return None

    return value

#-------------------------------------------------------------------------------


if __name__ == "__main__":

    Base.metadata.create_all(engine)

    #connection = engine.connect()
    '''
    session = Session()
    for item in glob.glob('data/WZ_subjects_extflag*.txt'):
        print("parsing {}".format(item))
        data = ascii.read(item)
        for row in data:
            row_data = {key:clean(value) for key,value in zip(row.columns, row.data)}
            session.add(Sed(**row_data))

        session.commit()
    session.close()
    '''

    session = Session()
    for item in glob.glob('data/WZ_removethesesubjects_*.txt'):
        print("deleting {}".format(item))
        for name in [line.strip() for line in open(item).readlines() if line.strip().startswith('J')]:
            session.query(Sed).filter(Sed.designation==name).delete()
            #connection.execute("""DELETE FROM sed WHERE designation='{}'""".format(name))
        session.commit()
    session.close()

#-------------------------------------------------------------------------------
