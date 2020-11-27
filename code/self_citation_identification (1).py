#!/usr/bin/env python
# coding: utf-8

#import os
from itertools import product
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from multiprocessing import Pool, Manager
import country_converter as coco
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import time

# In[118]:


class self_citation():
    
    def __init__(self, path):
        self.path = path
        self.read = False
        self.authors = {}
        self.names = set()
        self.articles = []
        self.files = os.listdir(self.path)
        self.countrymap = defaultdict(set)
        
        
    def __parse_author(self, line):
        author = line.lower().replace('au','').strip().replace(',','')
        
        self.names.add(author)
        if author not in self.authors:
            self.authors[author] = 9999
            
        return author
    
    
    def __parse_citation(self, line):
        cited = line.lower().replace('cr','').strip().split(',',1)[0]
        self.names.add(cited)
        return cited
    
    
    def __parse_year(self, line):
        return int(line.replace('PY ',''))
    
    
    def __parse_location(self, line):
        return line.lower().strip()
    
    def __parse_authorcountry(self, line, authormap):
        authors = line[line.find('[')+1: line.find(']')]
        authors = authors.split(';')
        country = line.split(',')[-1]
        country = self.__parse_country(country)
        for a in authors:
            a = a.strip()
            if a in authormap:
                self.countrymap[country].add( authormap[a] )
            else:
                print( a + " does not find match in " + str(authormap))
        return
    
    def __parse_country(self, country):
        country = country.replace('.','').strip()
        c = coco.convert(country, to='ISO3')
        if c == 'not found': # handle USA
            c = coco.convert(country.split(' ')[-1], to='ISO3')
        if c == 'not found': # england/scotland
            c = 'GBR'
        return c
    
    def __parse_line(self, line, au, af, c1, cr, pa, py, er, author_af_list, author_list, citation_list, year, location):
        if line.startswith('PT'):
            au = False
            af = False
            cr = False
            py = False
            er = False
            c1 = False
            author_list = []
            author_af_list = []
            citation_list = []
            year = None
            location = []
            
        elif line.startswith('AU'):
            au = True
        elif line.startswith('AF'):
            au = False
            af = True
            
        elif line.startswith('C1'):
            c1 = True
        elif line.startswith('RP'):
            c1 = False
            
        elif line.startswith('CR'):
            cr = True
        elif line.startswith('NR'):
            cr = False
            
        elif line.startswith('PA'):
            pa = True
        elif line.startswith('SN'):
            pa = False
            
        elif line.startswith('PY'):
            py = True
        elif line.startswith('ER'):
            er = True
            
            
        if au:
            author = self.__parse_author(line)
            author_list.append(author)
        
        if af:
            author_af_list.append(line.replace('AF','').strip())
            if (len(author_list) == len(author_af_list)):
                af = False
        if cr:
            cited = self.__parse_citation(line)
            citation_list.append(cited)
            
        if c1:
            afauthormap = {author_af_list[i]:author_list[i] for i in range(len(author_list))}
            self.__parse_authorcountry(line, afauthormap)
            
        if line.startswith('PY'):
            year = self.__parse_year(line)
            for a in author_list:
                if year < self.authors[a]:
                    self.authors[a] = year
                else: 
                    continue
        
        if pa:
            location.append(self.__parse_location(line))
        
        article_info = [author_list, citation_list, year, location]
        
        return au, af, c1, cr, pa, py, er, author_af_list, article_info

    
    
    def article_info_from_text(self):
        try:
            self.files.remove('.DS_Store')
        except:
            pass
        
        au = False
        af = False
        c1 = False
        cr = False
        pa = False
        py = False
        er = False
        author_list = []
        author_af_list = []
        citation_list = []
        year = None
        location = []
        
        start = time.time()
        for file in self.files:
            text = open(self.path+file)
            for line in text:
                au, af, c1, cr, pa, py, er, author_af_list, article_info = self.__parse_line(line, au, af,c1, cr, pa, py, er, author_af_list, author_list, citation_list, year, location)
                author_list, citation_list, year, location = article_info
                if er and py:
                    loc = ' '.join(location)
                    if loc.split(' ')[-1].strip() in ['states','republic']:
                        loc = loc.split(',')[-1].strip()
                    else:
                        loc = loc.split(' ')[-1].strip()
                    
                    article = (author_list,citation_list,year,loc)
                    career_stage = self.__generate_career_stage(article)
                    
                    self.articles.append((author_list,citation_list,year,loc,career_stage))
#                     article = {'AU':author_list,
#                                'CR':citation_list,
#                                'PY':year,
#                                'PA':loc}
#                     career_stage = self.__generate_career_stage(article)
#                     article['CA'] = career_stage
#                     self.articles.append(article)
                    er = False
        end = time.time()            
        self.read = True
        print(end-start)
        
        
    def __generate_career_stage(self, articles):
        authors = articles[0]
        authors_career_stage = [articles[2] - self.authors[name] for name in authors]
        return authors_career_stage
                    
    
    def build_matrix_from_articles(self, article_chunk, publication_location, career_stage):
            
        if publication_location:
            article_chunk = [article for article in article_chunk if article[3]==publication_location]
        else:
            pass
        
        for article in article_chunk:
            cited_row = []
            citing_col = []
            indices = []
            
            if career_stage:
                authors = [author for author,ca in zip(article[0],article[4]) if ca==career_stage]
            else:
                authors = article[0]
                
            cited_row = [self.names_dict[name] for name in article[1]]
            citing_col = [self.names_dict[name] for name in authors]
            indices = list(product(cited_row, citing_col))
            for i in indices:
                try: 
                    idx = self.matrix_index.index(i)
                    self.num_data[idx] += 1
                except:
                    self.matrix_index.append(i)
                    self.num_data.append(1)
                    
                    
    def __list_to_chunks(self, mylist, n_chunk):
        step = int(len(mylist)/n_chunk) + 1
        return [mylist[i:i+step] for i in range(0, len(mylist), step)]
    
                    
    def citation_matrix(self, publication_location=None, career_stage=None):
        if not self.read:
            self.article_info_from_text()
            
        self.names_dict = dict(zip(self.names,range(0,len(self.names))))
        
        manager = Manager()
        self.matrix_index = manager.list()
        self.num_data = manager.list()
        
        results = []
        p = Pool(4)
        article_chunks = self.__list_to_chunks(self.articles, 12)
        
        for article_chunk in article_chunks:
            p.apply_async(self.build_matrix_from_articles, args=(article_chunk,publication_location,career_stage,))
        p.close()
        p.join()
        
        if self.matrix_index:
            self.matrix_index = np.array(self.matrix_index)
            col = self.matrix_index[:,1]
            row = self.matrix_index[:,0]
            shape = (len(self.names_dict), len(self.names_dict))
            result = csr_matrix((self.num_data, (row, col)), shape = shape)
            author_num = len(set(self.matrix_index[:,1]))
            return result, author_num
        else:
            print('No Results')
            return None, None

                    
 
    def self_citation_level(self, matrix, author_number):
        sum_citation = matrix.toarray().sum()
        sum_self_citation = matrix.toarray().diagonal().sum()
        avg_self_citation = sum_self_citation/author_number
        self_citation_rate = sum_self_citation/sum_citation
        return avg_self_citation, self_citation_rate
    
    def by_author_location(self):
        countrylist, selfcitelist, ratelist = [], [], []
        matrix, author_number = self.citation_matrix()
        selfcite = matrix.toarray().diagonal()
        totalcite = matrix.toarray().sum(axis = 0)
        for country, authorlist in self.countrymap.items():
            indexlist = [self.names_dict[i] for i in list(authorlist)]
            author_num = len(indexlist)
            avg_selfcite = selfcite[indexlist].sum()/author_num
            avg_citation_rate = selfcite[indexlist].sum()/totalcite[indexlist].sum()
            countrylist.append(country)
            selfcitelist.append(avg_selfcite)
            ratelist.append(avg_citation_rate)
        by_author_location_da = pd.DataFrame(np.array([countrylist, 
                                                       selfcitelist,
                                                       ratelist]).T,
                                          columns=['author_location','avg_self_citation','self_citation_rate'])
        return by_author_location_da            


    def by_career_stage(self):
        self.career_year = []
        self.avg_self_citation_by_career_year = []
        self.self_citation_rate_by_career_year = []
        
        for y in set([i for article in x.articles for i in article[4] ]):
            matrix, author_number = self.citation_matrix(career_stage=y)
            avg_self_citation, self_citation_rate = self.self_citation_level(matrix, author_number)
            self.career_year.append(int(y))
            self.avg_self_citation_by_career_year.append(avg_self_citation)
            self.self_citation_rate_by_career_year.append(self_citation_rate)
            print('{} career year proceeced'.format(y))
        
        by_career_stage_da = pd.DataFrame(np.array([self.career_year, 
                                                    self.avg_self_citation_by_career_year, 
                                                    self.self_citation_rate_by_career_year]).T,
                                          columns=['career_year','avg_self_citation','self_citation_rate'])
        return by_career_stage_da
    
    
    def by_publication_location(self):
        self.publication_location = []
        self.avg_self_citation_by_publication_location = []
        self.self_citation_rate_by_publication_location = []
        
        for y in set([article[3] for article in x.articles]):
            matrix, author_number = self.citation_matrix(publication_location=y)
            avg_self_citation, self_citation_rate = self.self_citation_level(matrix, author_number)
            self.publication_location.append(y)
            self.avg_self_citation_by_publication_location.append(avg_self_citation)
            self.self_citation_rate_by_publication_location.append(self_citation_rate)
            print('publication location {} proceeced'.format(y))
        
        by_publication_location_da = pd.DataFrame(np.array([self.publication_location, 
                                                            self.avg_self_citation_by_publication_location, 
                                                            self.self_citation_rate_by_publication_location]).T,
                                          columns=['publication_location','avg_self_citation','self_citation_rate'])
        return by_publication_location_da
    
    
    def plot_by_publication_location(self):
        iso3 = coco.convert(names=self.publication_location, to='ISO3')
        iso3[iso3.index('not found')] = 'GBR'
        fig1 = go.Figure(data=go.Choropleth(
            locations = iso3,
            z = self.self_citation_rate_by_publication_location,
            text = self.publication_location,
            colorscale = 'Reds',
            autocolorscale=False,
            reversescale=False,
            marker_line_color='gray',
            marker_line_width=0.1,
            colorbar_title = 'self citation rate',),
            layout=dict(title = 'Self Citation Rate by Publication Location'))
        
        fig1.show()
        
        fig2 = go.Figure(data=go.Choropleth(
            locations = iso3,
            z = self.avg_self_citation_by_publication_location,
            text = self.publication_location,
            colorscale = 'Reds',
            autocolorscale=False,
            reversescale=False,
            marker_line_color='gray',
            marker_line_width=0.1,
            colorbar_title = 'average self citation',),
            layout=dict(title = 'Average Self Citation by Publication Location'))
        
        fig2.show()

        
    def plot_by_career_stage(self):
        fig, ax1 = plt.subplots()

        ax1.set_xlabel('Career Year')
        ax1.set_ylabel('avg_self_citation',color = 'red')
        ax1.plot(self.career_year, self.avg_self_citation_by_career_year, color = 'red', label = 'avg_self_citation')
        ax1.tick_params(axis='y', labelcolor='red')
        
        ax2 = ax1.twinx()
        
        ax2.set_ylabel('self_citation_rate',color = 'blue')
        ax2.plot(self.career_year, self.self_citation_rate_by_career_year, color = 'blue', label = 'self_citation_rate')
        ax2.tick_params(axis='y', labelcolor='blue')
        
        fig.set_size_inches(7,5)
        fig.legend(loc = 3,bbox_to_anchor=(0.35, 0.74))
        fig.suptitle('Self Citation Level by Career Stage')
        fig.show()

# In[119]:


x = self_citation('data/')


# In[120]:


x.article_info_from_text()


# In[8]:


by_career_stage = x.by_career_stage()


# In[61]:


by_career_stage


# In[8]:


# 1st
by_location = x.by_publication_location()


# In[121]:


# 2nd
by_location = x.by_publication_location()


# In[6]:


by_location


# In[7]:


x.plot_by_publication_location()


# In[ ]:


x.plot_by_career_stage()


# In[ ]:





# In[56]:


fig, ax1 = plt.subplots()

ax1.set_xlabel('Career Year')
ax1.set_ylabel('avg_self_citation',color = 'red')
ax1.plot(by_career_stage['career_year'], by_career_stage['avg_self_citation'], color = 'red', label = 'avg_self_citation')
ax1.tick_params(axis='y', labelcolor='red')

ax2 = ax1.twinx()

ax2.set_ylabel('self_citation_rate',color = 'blue')
ax2.plot(by_career_stage['career_year'], by_career_stage['self_citation_rate'], color = 'blue', label = 'self_citation_rate')
ax2.tick_params(axis='y', labelcolor='blue')

fig.set_size_inches(7,5)
fig.legend(loc = 3,bbox_to_anchor=(0.35, 0.74))
fig.suptitle('Self Citation Level by Career Stage')
fig.show()


# In[ ]:




