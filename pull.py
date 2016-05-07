from astropy.table import Table
import yaml
import os

from db_tables import load_connection, open_settings

SETTINGS = yaml.load(open(os.path.join(os.environ['HOME'], 'dd_configure.yaml')))
print SETTINGS

Session, engine = load_connection(SETTINGS['CONNECTION_STRING'], echo=False)

results = engine.execute("""SELECT
    sed.designation,
    GROUP_CONCAT(DISTINCT(classifications.classified_as) SEPARATOR ';') classification,
    subjects.ddid,
    subjects.state,
    subjects.im_2massj as 2mass_imagj,
    subjects.im_2massk as 2mass_image_k,
    sed.ra,
    sed.dec,
    sed.glon,
    sed.glat,
    sed.w1mpro,
    sed.w2mpro,
    sed.w3mpro,
    sed.w4mpro,
    sed.j_m_2mass,
    sed.h_m_2mass,
    sed.k_m_2mass,
    urat.pmRA,
    urat.pmDE,
    urat.Bmag,
    urat.Vmag,
    urat.rmag,
    urat.imag,
    urat.gmag
        FROM sed
                JOIN subjects ON sed.designation = subjects.wise_id
                JOIN classifications ON subjects.ddid = classifications.ddid
                LEFT JOIN urat ON sed.id = urat.file_id
                WHERE classifications.classified_as IS NOT NULL
                GROUP BY sed.designation;""")

keys = results.keys()
print keys
datarows = []
for item in results:
    item = item.values()
    for i, column in enumerate(item):
        if str(column).strip().startswith('http'):
            print column
            item[i] = """<a href="{0}" target="_blank"><img width="100" src="{0}"><a>""".format(column.strip())
    datarows.append(item)

#datarows = [item for item in results]

t = Table(rows=datarows, names=keys, meta={'Name':'COS HEADER TABLE'})
t.write('composite_table.html', format='jsviewer')
