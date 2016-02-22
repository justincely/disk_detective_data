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



#-------------------------------------------------------------------------------
