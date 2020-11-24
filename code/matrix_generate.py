#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from itertools import product
import numpy as np
from scipy.sparse import csr_matrix


# In[2]:


class self_citation():
    
    def __init__(self, path):
        self.path = path
        self.read = False
        
    
    def names_dict_generator(self):
        """
        Read files and build author dictionary and find minimum publish year
        """
        self.authors = {}
        self.names = set()
        self.files = os.listdir(self.path)
        try:self.files.remove('.DS_Store')
        except:pass
        
        for file in self.files:
            with open(self.path+file) as text:
                au = 0
                cr = 0
                author_list = []
                for line in text:
                    # If 
                    if line.startswith('PT'):
                        author_list = []
                        
                    if line.startswith('AU'):
                        au=1
                    elif line.startswith('AF'):
                        au=0
                        
                    if au:
                        author = line.lower().replace('au','').strip().replace(',','')
                        author_list.append(author)
                        self.names.add(author)
                        if author in self.authors:
                            continue
                        else: self.authors[author] = 9999
                    
                    if line.startswith('CR'):
                        cr=1
                    elif line.startswith('NR'):
                        cr=0
                        
                    if cr:
                        cited = line.lower().replace('cr','').strip().split(',',1)[0]
                        self.names.add(cited)

                    if line.startswith('PY'):
                        year = int(line.replace('PY ',''))
                        for a in author_list:
                            if year < self.authors[a]:
                                self.authors[a] = year
                            else: continue
        self.names_dict = dict(zip(self.names,range(0,len(self.names))))
        self.read = True
                 
    
    def citation_matrix(self, location=None, career_stage=None):
        if not self.read:
            self.names_dict_generator()
            
        self.location = location
        self.career_stage = career_stage
        self.matrix_index = []
        self.num_data = []
        
        for file in self.files:
            with open(self.path+file) as text:
                au = 0
                cr = 0
                for line in text:
                    if line.startswith('PT'):
                        citing_col = []
                        cited_row = []
                        data = []
                        author_list = []
                        py = False
                        
                    if line.startswith('AU'):
                        au=1
                    elif line.startswith('AF'):
                        au=0
                        
                    if au:
                        author = line.lower().replace('au','').strip().replace(',','')
                        author_list.append(author)
                        citing_col.append(self.names_dict[author])
                    
                    # Select location
                    # if line.startswith('C1'):
                        
                    
                    if line.startswith('CR'):
                        cr=1
                    elif line.startswith('NR'):
                        cr=0
                        
                    if cr:
                        cited = line.lower().replace('cr','').strip().split(',',1)[0]
                        cited_row.append(self.names_dict[cited])
                    
                    if line.startswith('PY'):
                        py = True
                        if self.career_stage is not None:
                            year = int(line.replace('PY ',''))
                            for author in author_list:
                                if (year - self.authors[author]) in range(self.career_stage[0],self.career_stage[1]):
                                    continue
                                else:
                                    citing_col.remove(self.names_dict[author])
                    
                    if line.startswith('ER'):
                        if not py:
                            continue
                            
                        indices = list(product(cited_row, citing_col))
                        for i in indices:
                            try: 
                                idx = self.matrix_index.index(i)
                                self.num_data[idx] += 1
                            except:
                                self.matrix_index.append(i)
                                self.num_data.append(1)
                        indices = []        
        
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
        


# In[3]:


x = self_citation('data/')


# In[4]:


x.names_dict_generator()


# In[5]:


matrix1, author_num1 = x.citation_matrix(career_stage=(0,1))


# In[6]:


matrix2, author_num2 = x.citation_matrix(career_stage=(1,2))


# In[7]:


matrix3, author_num3 = x.citation_matrix(career_stage=(2,3))


# In[8]:


matrix4, author_num4 = x.citation_matrix(career_stage=(3,4))


# In[9]:


matrix5, author_num5 = x.citation_matrix(career_stage=(4,5))


# In[10]:


def avg_citation_level(matrix, author_number):
    sum_self_citation = matrix.toarray().diagonal().sum()
    self_citation_level = sum_self_citation/author_number
    return self_citation_level


# In[19]:


print('Average Self Citation Level - Career < 1 year:', avg_citation_level(matrix1, author_num1), '\n' 'Author Number:', author_num1)


# In[20]:


print('Average Self Citation Level - Career 1-2 year:', avg_citation_level(matrix2, author_num2), '\n' 'Author Number:', author_num2)


# In[21]:


print('Average Self Citation Level - Career 2-3 year:', avg_citation_level(matrix3, author_num3), '\n' 'Author Number:', author_num3)


# In[22]:


print('Average Self Citation Level - Career 3-4 year:', avg_citation_level(matrix4, author_num4), '\n' 'Author Number:', author_num4)


# In[23]:


print('Average Self Citation Level - Career 4-5 year:', avg_citation_level(matrix5, author_num5), '\n' 'Author Number:', author_num5)


# In[ ]:




