#!/usr/bin/env python
# coding: utf-8

# In[505]:


import dask.dataframe as dd
import pandas as pd
import pandas as pd


# In[506]:


# Domestic Data Load
vaers_vax_data = dd.read_csv('dataset/*VAERSVAX.csv',  delimiter=',',encoding='ISO-8859-1')
vaers_vax_data.to_csv("dataset/testing.csv", single_file=True)
vaers_symptom_data = dd.read_csv('dataset/*VAERSSYMPTOMS.csv', delimiter=',',encoding='utf-8', dtype={'SYMPTOM5': 'object'})
vaers_data = dd.read_csv('dataset/*VAERSDATA.csv',  delimiter=',',encoding='ISO-8859-1',
       dtype={'ALLERGIES': 'object',
       'BIRTH_DEFECT': 'object',
       'CUR_ILL': 'object',
       'DATEDIED': 'object',
       'ER_ED_VISIT': 'object',
       'ER_VISIT': 'object',
       'HOSPITAL': 'object',
       'L_THREAT': 'object',
       'OFC_VISIT': 'object',
       'OTHER_MEDS': 'object',
       'RPT_DATE': 'object',
       'TODAYS_DATE': 'object',
       'X_STAY': 'object'})

# Non Domestic Data Load
vaers_vax_data_nondem = dd.read_csv('dataset/non_domestic/*VAERSVAX.csv', delimiter=',', encoding='ISO-8859-1', dtype={'VAX_SITE': 'object'})
vaers_symptom_data_nondem = dd.read_csv('dataset/non_domestic/*VAERSSYMPTOMS.csv', delimiter=',', encoding='utf-8', dtype={'SYMPTOM5': 'object'})
vaers_data_nondem = dd.read_csv('dataset/non_domestic/*VAERSDATA.csv', delimiter=',', encoding='ISO-8859-1',
                                dtype={'ALLERGIES': 'object',
       'BIRTH_DEFECT': 'object',
       'CUR_ILL': 'object',
       'DATEDIED': 'object',
       'ER_ED_VISIT': 'object',
       'L_THREAT': 'object',
       'OFC_VISIT': 'object',
       'TODAYS_DATE': 'object',
       'X_STAY': 'object'})


# # Parsing data and Displaying the tails of the data

# In[507]:


df = vaers_vax_data.compute()


# In[508]:


print(df.shape)


# In[509]:


vaers_vax_data.tail()


# In[510]:


vaers_symptom_data.tail()


# In[511]:


vaers_data.tail()


# In[512]:


vaers_vax_data_nondem.tail()


# In[513]:


vaers_symptom_data_nondem.tail()


# In[514]:


vaers_data_nondem.tail()


# # Computing Number of Deaths For Each Vaccine Manufacturer (Domestic)

# In[515]:


# Get the set of all VAX_NAMES
vaccines = vaers_vax_data['VAX_NAME'].unique()
vaccines = vaccines.to_frame()


# In[516]:


vaccines.tail()


# In[517]:


vaccine_deaths = vaers_data.dropna(subset=['DIED'])
vaccine_deaths = vaccine_deaths[['VAERS_ID', 'SEX' , 'AGE_YRS', 'DIED', 'VAX_DATE', 'ONSET_DATE','DATEDIED', 'NUMDAYS']]


# In[518]:


vaccine_deaths.tail()


# In[519]:


vaccine_deaths = vaccine_deaths.merge(vaers_vax_data, on=['VAERS_ID'], how='inner')


# In[520]:


vaccine_deaths.to_csv('dataset/vaccine_deaths_extracted.csv', single_file=True)

