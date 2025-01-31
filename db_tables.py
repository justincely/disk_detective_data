\

import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, Column, Index, Integer, String, Float, Boolean, Numeric
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, relationship, backref

try:
    import yaml
except ImportError:
    from .yaml import yaml

__all__ = ['open_settings', 'load_connection']

Base = declarative_base()

#-------------------------------------------------------------------------------

def open_settings(config_file=None):
    """ Parse config file and load settings
    If no config file is supplied, the configuration file will assume to be
    located at '~/configure.yaml'.
    Parameters
    ----------
    config_file : str, optional
        yaml file containing configuration settings.
    Returns
    -------
    settings : dict
        dictionary of all settings
    """

    config_file = config_file or os.path.join(os.environ['HOME'], "configure.yaml")

    with open(config_file, 'r') as f:
        settings = yaml.load(f)

    return settings

#-------------------------------------------------------------------------------

def load_connection(connection_string, echo=False):
    """Create and return a connection to the database given in the
    connection string.
    Parameters
    ----------
    connection_string : str
        A string that points to the database conenction.  The
        connection string is in the following form:
        dialect+driver://username:password@host:port/database
    echo : bool
        Show all SQL produced.
    Returns
    -------
    session : sesson object
        Provides a holding zone for all objects loaded or associated
        with the database.
    engine : engine object
        Provides a source of database connectivity and behavior.
    """

    engine = create_engine(connection_string, echo=echo)
    Session = sessionmaker(bind=engine)

    return Session, engine

#-------------------------------------------------------------------------------

class Sed(Base):
    __tablename__ = 'sed'

    id = Column(Integer, primary_key=True)

    designation = Column(String(20))
    ra = Column(Numeric(11, 7))
    dec = Column(Numeric(11, 7))
    glon = Column(Numeric(11, 7))
    glat = Column(Numeric(11, 7))
    w1mpro = Column(Float)
    w1sigmpro = Column(Float)
    w2mpro = Column(Float)
    w2sigmpro = Column(Float)
    w3mpro = Column(Float)
    w3sigmpro = Column(Float)
    w4mpro = Column(Float)
    w4sigmpro = Column(Float)
    j_m_2mass = Column(Float)
    j_msig_2mass = Column(Float)
    h_m_2mass = Column(Float)
    h_msig_2mass = Column(Float)
    k_m_2mass = Column(Float)
    k_msig_2mass = Column(Float)

    __table_args__ = (Index('idx_designation', 'designation', unique=True), )

#-------------------------------------------------------------------------------

class Hip(Base):
    __tablename__ = 'hip'

    id = Column(Integer, primary_key=True)

    Plx = Column(Float)
    pmDE = Column(Float)
    pmRA = Column(Float)
    e_pmRA = Column(Float)
    e_pmDE = Column(Float)
    Hpmag = Column(Float)
    e_Hpmag = Column(Float)

    file_id = Column(Integer, ForeignKey('sed.id'))

#-------------------------------------------------------------------------------

class Urat(Base):
    __tablename__ = 'urat'

    id = Column(Integer, primary_key=True)

    pmRA = Column(Float)
    pmDE = Column(Float)
    e_pm = Column(Float)

    Bmag = Column(Float)
    Vmag = Column(Float)
    rmag = Column(Float)
    imag = Column(Float)
    gmag = Column(Float)

    e_Bmag = Column(Float)
    e_Vmag = Column(Float)
    e_rmag = Column(Float)
    e_imag = Column(Float)
    e_gmag = Column(Float)

    file_id = Column(Integer, ForeignKey('sed.id'))

#-------------------------------------------------------------------------------

class Subjects(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True)

    ddid = Column(String(24))
    wise_id = Column(String(20))
    state = Column(String(20))
    im_2massj = Column(String(100))
    im_2massk = Column(String(100))

    #designation = Column(Integer, ForeignKey('sed.designation'))
    __table_args__ = (Index('idx_designation', 'wise_id'), )
    __table_args__ = (Index('idx_ddid', 'ddid'), )

#-------------------------------------------------------------------------------

class Classifications(Base):
    __tablename__ = 'classifications'

    id = Column(Integer, primary_key=True)

    ddid = Column(String(24))
    classified_as = Column(String(20))

    #designation = Column(Integer, ForeignKey('sed.designation'))
    __table_args__ = (Index('idx_designation', 'ddid'), )

#-------------------------------------------------------------------------------

class Ais(Base):
    __tablename__ = 'ais'

    id = Column(Integer, primary_key=True)

    FUV = Column(Float)
    NUV = Column(Float)
    e_FUV = Column(Float)
    e_NUV = Column(Float)

    file_id = Column(Integer, ForeignKey('sed.id'))

#-------------------------------------------------------------------------------

class Hip_main(Base):
    __tablename__ = 'hip_main'

    id = Column(Integer, primary_key=True)

    BTmag = Column(Float)
    VTmag= Column(Float)
    e_BTmag = Column(Float)
    e_VTmag = Column(Float)

    file_id = Column(Integer, ForeignKey('sed.id'))

#-------------------------------------------------------------------------------

class Iphas(Base):
    __tablename__ = 'iphas'

    id = Column(Integer, primary_key=True)

    r = Column(Float)
    rErr = Column(Float)
    i = Column(Float)
    iErr = Column(Float)
    ha = Column(Float)
    haErr = Column(Float)

    file_id = Column(Integer, ForeignKey('sed.id'))

#-------------------------------------------------------------------------------
