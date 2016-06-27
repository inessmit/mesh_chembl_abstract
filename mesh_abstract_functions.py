
# coding: utf-8

# In[1]:

import requests
from lxml import etree
import sqlalchemy as alchemy
import logging
from Bio import Entrez


# In[ ]:

def create_db_conn(filepath):
    """Open file with login details and create connection to chempro.
    kwargs: filepath -- string"""
    
    with open(filepath) as f:
        login = f.read().strip()
    engine = alchemy.create_engine(login)
    logging.info('create_db_conn -- created connection to Oracle')
    return engine

# Alternative to this function you can establish connection to chempro:
# Use alchemy.create_engine('oracle://{}:{}@ora-vm5-023.ebi.ac.uk:1531/Chempro'.format(user, password))


# In[ ]:

def create_db_tables(engine, annotation_table, descriptors_table, qualifiers_table):
    """Create tables for storing mesh annotations in my own Oracle area. Only needs to be done once.
    kwargs: engine -- db engine
            annotation_table -- string, name of the table for storing the pmid together with ids for descriptors and qualifiers
            descriptors_table -- string, name of the table for storing descriptor unique id together with descriptor heading/text
            qualifiers_table -- string, name of the table for storing qualifier unique id together with qualifier heading/text """
    
    # create table mesh annotations

    engine.execute('''
    create table {}
    (
      year int
    , pmid int
    , descriptor_ui varchar2(50)
    , qualifier_ui varchar(50)
    , primary key (pmid,descriptor_ui)
    )
    '''.format(annotation_table))
    
    # create table mesh descriptors

    engine.execute('''
    create table {}
    (
      descriptor_ui varchar(50)
    , descriptor_text varchar(200)
    , primary key(descriptor_ui, descriptor_text))
    '''.format(descriptors_table))
    
    # create table mesh qualifiers

    engine.execute('''
    create table {}
    (
    qualifier_ui varchar(50)
    , qualifier_text varchar(200)
    , primary key(qualifier_ui, qualifier_text)
    )
    '''.format(qualifiers_table))
    
    logging.info('create_db_tables -- created tables')


# In[ ]:

def get_pmids(engine, year):
    """Gets list of pmids from ChEMBL for a given year
    kwargs: engine -- db connection
            year -- year of publication (arbitrary split of the data)"""
    
    ids = ''
    sql = '''select pubmed_id from chembl_21.docs where pubmed_id is not null and year = {}'''.format(str(year))
    
    result = engine.execute(sql).fetchall()
    result_len = len(result)
    #ids = ','.join(str(i[0]) for i in result)
    ids = [i[0] for i in result]
    
    logging.info('get_pmids -- got pmids')
    
    return ids


# In[ ]:

def entrez_post_fetch(ids, email):
    '''Use Biopython Entrez module to post ids to NCBI server (Entrez automatically uses HTTP post if = number of ids > 200) 
    and use efetch with history (taking webenv and key data from epost returned XML) to get result XML. Return etree element.
    kwargs: ids -- list of pubmed ids
            email -- email address should always be specified for using Entrez Web Services'''
    
    Entrez.email = email # Should always be specified when using Entrez Web Services, otherwise run the risk of being blocked in case of overuse without them being able to contact you.
    
    result0 = Entrez.epost('pubmed', id=','.join([str(my_id) for my_id in ids])).read()
    tree0 = etree.fromstring(result0)
    key, env = tree0.xpath('//QueryKey/text()')[0], tree0.xpath('//WebEnv/text()')[0]
    logging.info('got key and webenv info')

    result1 = Entrez.efetch(db='pubmed', retmode='xml', webenv = env, query_key = key).read()
    tree1 = etree.fromstring(result1)
    logging.info('got result from efetch')
    return tree1


# In[ ]:

def clear_all_tables(engine, annotation_table, descriptors_table, qualifiers_table):
    
    '''Delete all records in the tables mesh_descriptors, mesh_qualifiers, and mesh_annotations tables. Leaves empty tables.
    kwargs: engine -- db engine
            annotation_table -- string, name of the table for storing the pmid together with ids for descriptors and qualifiers
            descriptors_table -- string, name of the table for storing descriptor unique id together with descriptor heading/text
            qualifiers_table -- string, name of the table for storing qualifier unique id together with qualifier heading/text'''

        
    engine.execute('delete from {}'.format(annotation_table))
    engine.execute('delete from {}'.format(descriptors_table))
    engine.execute('delete from {}'.format(qualifier_table))
    
    logging.info('clear_all_tables -- cleared all tables')


# In[ ]:

def save_data(engine, tree, annotation_table, descriptors_table, qualifiers_table):
    """For each article(pmid) get mesh annotations, i.e. descriptors and optionally qualifiers. Insert into tables in user area which were created previously.
    kwargs: engine -- db engine
            tree -- lxml Etree XML element from efetch (see post_fetch function)"""
    
    
    for article in tree.xpath('PubmedArticle'):
            pmid = article.xpath('./MedlineCitation/PMID/text()')[0]
            
            try:
                headings = article.xpath('.//MeshHeadingList')[0].iterchildren()
            except IndexError:
                logging.info('save_data: IndexError exception for ' + pmid + ' -- article XML does not have MeshHeadingList') # not all articles have MeSH terms
                continue

            for heading in headings:

                qualifier_ui, qualifier_text = ('','') # make this empty because not all descriptors have qualifiers
                full_fields = [(i.tag, i.get('UI'), i.text) for i in heading.iterchildren()]

                descriptor_ui, descriptor_text  = (full_fields[0][1].replace("'", "''"), full_fields[0][2].replace("'", "''"))

                engine.execute('''
                insert into {0} (descriptor_ui, descriptor_text)
                select '{1}', '{2}' from dual
                where not exists(select * from {0} where (descriptor_ui = '{1}' and descriptor_text = '{2}'))
                '''.format(descriptors_table, descriptor_ui, descriptor_text))
                
                if len(full_fields) > 1: # This is in the case there are qualifiers.
                    
                    qualifier_ui, qualifier_text  = (full_fields[1][1].replace("'", "''"), full_fields[1][2].replace("'", "''"))

                    engine.execute('''
                    insert into {0} (qualifier_ui, qualifier_text)
                    select '{1}', '{2}' from dual
                    where not exists(select * from {0} where (qualifier_ui = '{1}' and qualifier_text = '{2}'))
                    '''.format(qualifiers_table, qualifier_ui, qualifier_text))

                    engine.execute('''
                    insert into {0} (pmid, descriptor_ui, qualifier_ui)
                    select {1}, '{2}', '{3}' from dual
                    where not exists(select pmid, descriptor_ui from {0} where (pmid = {1} and descriptor_ui = '{2}'))
                    '''.format(annotation_table, pmid, descriptor_ui, qualifier_ui))
                    
                else: # This is in the case there are no qualifiers.

                    engine.execute('''
                    insert into {0} (pmid, descriptor_ui)
                    select {1}, '{2}' from dual
                    where not exists(select pmid, descriptor_ui from {0} where (pmid = {1} and descriptor_ui = '{2}'))
                    '''.format(annotation_table, pmid, descriptor_ui))
    
    logging.info('save_data -- inserted mesh annotations into oracle tables')

    

