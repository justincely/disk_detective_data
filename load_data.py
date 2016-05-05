import glob
from sqlalchemy import create_engine
from astropy.io import ascii
import os
from astroquery.vizier import Vizier
import astropy.coordinates as coord
import astropy.units as u
import numpy as np
import multiprocessing as mp

import yaml
import json

from db_tables import load_connection, open_settings
from db_tables import Sed, Urat, Hip
from db_tables import Base
from db_tables import Subjects, Classification

SETTINGS = yaml.load(open(os.path.join(os.environ['HOME'], 'dd_configure.yaml')))
print SETTINGS

Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=True)

#-------------------------------------------------------------------------------

def clean(value):
    if np.isnan(value):
        return None
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

def mp_insert(args):
    """Wrapper function to parse arguments and pass into insertion function
    Parameters
    ----------
    args, tuple
        filename, table, function, (foreign key, optional)
    """

    resolve(*args)

#-------------------------------------------------------------------------------

def resolve(id, ra, dec, catalog, table, columns):
    session = Session()

    res = Vizier(catalog=catalog, columns=columns)

    lines = res.query_region(coord.SkyCoord(ra=ra,
                                            dec=dec,
                                            unit=(u.deg, u.deg), frame='icrs'),
                                            radius="1s")

    if len(lines) == 1:
        data = {key:clean(lines[0][key].item()) for key in lines[0].keys() if not key in ['_RAJ2000', '_DEJ2000']}
        print data
        session.add(table(**data))
    else:
        session.add(table(**{'file_id':id}))

    session.commit()
    session.close()

#-------------------------------------------------------------------------------

def insert_wise_data():
    session = Session()

    with open('data/wise_subjects.json') as infile:
        for line in infile:
            data = json.loads(line)
            to_insert = {'ddid': data['_id']['$oid'],
                         'wise_id': data['metadata']['wise_id'],
                         'state': data['state']}

            session.add(Subjects(**to_insert))

        session.commit()

    with open('data/wise_classifications.json') as infile:
        for line in infile:
            data = json.loads(line)
            ddid = data['_id']['$oid']

            for line in data['annotations']:
                if 'classified_as' in line:
                    to_insert = {'ddid': ddid,
                                 'classified_as': line['classified_as']}
                    session.add(Classifications(**to_insert))
        session.commit()

    session.close()

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    Base.metadata.create_all(engine)

    insert_wise_data()

    sys.exit()


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
    """
    session = Session()
    for item in glob.glob('data/WZ_removethesesubjects_*.txt'):
        print("deleting {}".format(item))
        for name in [line.strip() for line in open(item).readlines() if line.strip().startswith('J')]:
            session.query(Sed).filter(Sed.designation==name).delete()
        session.commit()
    session.close()
    """

    session = Session()
    results = session.query(Sed.id, Sed.ra, Sed.dec)
    session.close()

    pool = mp.Pool(processes=4)

    #-- Hipparcos catalog for proper motion and parallax
    cat = "I/311/hip2"
    columns = ['Plx', 'pmRA', 'pmDE', 'e_pmRA', 'e_pmDE', 'Hpmag', 'e_Hpmag']
    args = ((row[0], row[1], row[2], cat, Hip, columns) for row in results)
    pool.map(mp_insert, args)

    #-- Urat catalog for magnitudes
    cat = "I/329/urat1"
    columns = ['pmRA',
               'pmDE',
               'e_pm',
               'Bmag',
               'e_Bmag',
               'Vmag',
               'e_Vmag',
               'gmag',
               'e_gmag'
               'rmag',
               'e_rmag',
               'imag',
               'e_imag']
    args = ((row[0], row[1], row[2], cat, Urat, columns) for row in results)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------
