
# coding: utf-8

# In[1]:

import logging
import entrez_mesh_functions as mf
from datetime import date


# In[ ]:

logging.basicConfig(format='%(asctime)s %(message)s', filename='mesh.log',level=logging.DEBUG)
logging.info('Started')


# In[2]:

# Update this path to point to file with alchemy login details

engine = mf.create_db_conn('/homes/ines/alchemy_ines_login.txt')


# In[ ]:

# Get all available years from ChEMBL, by year is an arbitrary division really, could be simplified and just do in batches based on pmid
#chembl_years = [result[0] for result in engine.execute('select distinct year from chembl_21.docs where pubmed_id is not null').fetchall()]

# As an example, do a few years
chembl_years = [2005, 2011, 2013]


# In[8]:

table_names = [name.format(date.today()) for name in ['mesh_annotations_{:%m%d%Y}', 'mesh_descriptors_{:%m%d%Y}', 'mesh_qualifiers_{:%m%d%Y}']]


# In[9]:

mf.create_db_tables(engine, annotation_table = table_names[0], descriptors_table = table_names[1], qualifiers_table=table_names[2])


# In[ ]:

#mf.clear_all_tables(engine)


# In[ ]:

for current_year in chembl_years:
    logging.info('Now starting year {} '.format(str(current_year)))

    my_ids = mf.get_pmids(engine, year = current_year)
    my_tree = mf.entrez_post_fetch(ids= my_ids, email = 'ines@ebi.ac.uk')
    mf.save_data(engine, tree = my_tree, annotation_table = table_names[0], descriptors_table = table_names[1], qualifiers_table=table_names[2])

    logging.info('Now finished year {} '.format(str(current_year)))


# In[ ]:

logging.info('Finished script')

