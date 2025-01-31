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
from db_tables import Sed, Urat, Hip, Ais, Hip_main, Iphas
from db_tables import Base
from db_tables import Subjects, Classifications

SETTINGS = yaml.load(open(os.path.join(os.environ['HOME'], 'dd_configure.yaml')))
print SETTINGS

Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=True)

#-------------------------------------------------------------------------------

def clean(value):
    if isinstance(value, float):
        if np.isnan(value):
            return None
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

def resolve(id, ra, dec, catalog, table):
    Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)
    session = Session()
    already_found = [result.file_id for result in session.query(table).filter(table.file_id == id)]

    if len(already_found):
        print("Resolving {} {} {}: Already Resolved".format(id, ra, dec))
    else:
        lines = catalog.query_region(coord.SkyCoord(ra=ra,
                                                    dec=dec,
                                                    unit=(u.deg, u.deg), frame='icrs'),
                                                    radius="1s")
        #-- index into first (only) table
        if len(lines) > 1:
            lines = lines[0]

        if len(lines) >= 1:
            print("Resolving {} {} {}, {} Matches Found".format(id, ra, dec, len(lines)))

            #strip off everything but the first:
            data = {key:clean(lines[0][key][0].item()) for key in lines[0].keys() if not key in ['_RAJ2000', '_DEJ2000']}
            data['file_id'] = id
            print data
            session.add(table(**data))
            session.commit()

        else:
            print("Resolving {} {} {}: No Data found".format(id, ra, dec))

    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def parse_subjects(txtline):
    Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)
    session = Session()

    data = json.loads(txtline)

    to_insert = {'ddid': data['_id']['$oid'],
                'wise_id': data['metadata'].get('wise_id', None),
                'state': data['state'],
                'im_2massj': data['location'].get('2massj', None),
                'im_2massk': data['location'].get('2massk', None)}

    session.add(Subjects(**to_insert))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def parse_classifications(txtline):
    Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)
    session = Session()

    data = json.loads(txtline)
    ddid = data['subject_ids'][0]['$oid']

    for line in data['annotations']:
        if 'classified_as' in line:
            to_insert = {'ddid': ddid,
                         'classified_as': line['classified_as']}
            session.add(Classifications(**to_insert))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def parse_initial(row):
    Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)
    session = Session()

    row_data = {key:clean(value) for key,value in zip(row.columns, row.data)}
    session.add(Sed(**row_data))

    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def rm_name(name):
    Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)
    session = Session()

    session.query(Sed).filter(Sed.designation==name).delete()
    session.commit()
    session.close()
    engine.dispose()

#-------------------------------------------------------------------------------

def insert_initial_data():
    pool = mp.Pool(processes=8)

    for item in glob.glob('data/WZ_subjects_extflag*.txt'):
        print("parsing {}".format(item))
        data = [row for row in ascii.read(item)]
        pool.map(parse_initial, data)

    for item in glob.glob('data/WZ_removethesesubjects_*.txt'):
        print("deleting {}".format(item))
        all_names = [line.strip() for line in open(item).readlines() if line.strip().startswith('J')]
        pool.map(rm_name, all_names)

#-------------------------------------------------------------------------------

def insert_wise_data():
    pool = mp.Pool(processes=8)

    with open('data/wise_subjects.json') as infile:
        pool.map(parse_subjects, infile.readlines())

    with open('data/wise_classifications.json') as infile:
        pool.map(parse_classifications, infile.readlines())

#-------------------------------------------------------------------------------

if __name__ == "__main__":

    Base.metadata.create_all(engine)

    insert_initial_data()
    insert_wise_data()

    sys.exit()

    session = Session()
    results = session.query(Sed.id, Sed.ra, Sed.dec)
    session.close()

    pool = mp.Pool(processes=8)

    #-- Hipparcos catalog for proper motion and parallax
    cat = "I/311/hip2"
    columns = ['Plx', 'pmRA', 'pmDE', 'e_pmRA', 'e_pmDE', 'Hpmag', 'e_Hpmag']
    catalog = Vizier(catalog=cat, columns=columns)
    args = ((row[0], row[1], row[2], catalog, Hip) for row in results)
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
    catalog = Vizier(catalog=cat, columns=columns)
    args = ((row[0], row[1], row[2], catalog, Urat) for row in results)
    pool.map(mp_insert, args)

    #-- Hipparcos catalog for proper motion and parallax
    cat = "II/312/ais"
    columns = ['FUV', 'NUV', 'e_FUV', 'e_NUV']
    catalog = Vizier(catalog=cat, columns=columns)
    args = ((row[0], row[1], row[2], catalog, Ais) for row in results)
    pool.map(mp_insert, args)

    #-- Hipparcos catalog for proper motion and parallax
    cat = "I/239/hip_main"
    columns = ['BTmag', 'VTmag', 'e_BTmag', 'e_VTmag']
    catalog = Vizier(catalog=cat, columns=columns)
    args = ((row[0], row[1], row[2], catalog, Hip_main) for row in results)
    pool.map(mp_insert, args)

    #-- Hipparcos catalog for proper motion and parallax
    cat = "II/321/iphas2"
    columns = ['r', 'rErr', 'i', 'iErr', 'ha', 'haErr']
    catalog = Vizier(catalog=cat, columns=columns)
    args = ((row[0], row[1], row[2], catalog, Iphas) for row in results)
    pool.map(mp_insert, args)

#-------------------------------------------------------------------------------
