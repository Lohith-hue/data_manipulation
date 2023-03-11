#!/usr/bin/env python
# coding: utf-8

# In[47]:


import xml.etree.ElementTree as ET
import pandas as pd


# In[89]:


def transform_xml(xml_doc):
    attr = xml_doc.attrib
    for xml in xml_doc.findall('./delete//'):
        dict = attr.copy()
        dict.update(xml.attrib)

        yield dict

etree = ET.parse('oscfile.osc')

eroot = etree.getroot()

trans = transform_xml(eroot)

delete_df = pd.DataFrame(list(trans))

delete_df


# In[90]:


delete_df.rename(columns = {"type":"member_type"}, inplace="True")

delete_df = delete_df.drop(['generator','lat','lon'], axis = 1)


# In[91]:


delete_df


# In[92]:


columns = ['type']
delete_tag = pd.DataFrame(columns = columns)

for child in eroot.findall("./delete//"):
    print(child.tag)
    delete_tag = delete_tag.append(pd.Series(child.tag , index = columns), ignore_index = True)


# In[93]:


delete_tdf = pd.concat([delete_tag, delete_df], axis=1, join='inner')


# In[95]:


delete_tdf = delete_tdf.replace(['tag', 'nd','member','relation'],[None, None,None,None])
cols = ['type','id', 'timestamp','uid','user','changeset']
delete_tdf.loc[:,cols] = delete_tdf.loc[:,cols].ffill()
delete_tdf['Mod_type'] = 'delete'
delete_tdf


# In[ ]:





# In[100]:


def transform_xml(xml_doc):
    attr = xml_doc.attrib
    for xml in xml_doc.findall('./create//'):
        dict = attr.copy()
        dict.update(xml.attrib)

        yield dict

etree = ET.parse('oscfile.osc')

eroot = etree.getroot()

trans = transform_xml(eroot)

create_df = pd.DataFrame(list(trans))

create_df


# In[101]:


create_df = create_df.drop(['generator','lat','lon'], axis = 1)


# In[102]:


create_df


# In[103]:


columns = ['type']
create_tag = pd.DataFrame(columns = columns)

for child in eroot.findall("./create//"):
    print(child.tag)
    create_tag = create_tag.append(pd.Series(child.tag , index = columns), ignore_index = True)


# In[104]:


create_tdf = pd.concat([create_tag, create_df], axis=1, join='inner')
create_tdf


# In[106]:


create_tdf = create_tdf.replace(['tag', 'nd','member','relation'],[None, None,None,None])
cols = ['type','id', 'timestamp']
create_tdf.loc[:,cols] = create_tdf.loc[:,cols].ffill()
create_tdf['Mod_type'] = 'create'
create_tdf


# In[ ]:





# In[107]:


def transform_xml(xml_doc):
    attr = xml_doc.attrib
    for xml in xml_doc.findall('./modify//'):
        dict = attr.copy()
        dict.update(xml.attrib)

        yield dict

etree = ET.parse('oscfile.osc')

eroot = etree.getroot()

trans = transform_xml(eroot)

modify_df = pd.DataFrame(list(trans))

modify_df


# In[108]:


modify_df = modify_df.drop(['generator','lat','lon'], axis = 1)


# In[109]:


modify_df


# In[110]:


columns = ['type']
modify_tag = pd.DataFrame(columns = columns)

for child in eroot.findall("./modify//"):
    print(child.tag)
    modify_tag = modify_tag.append(pd.Series(child.tag , index = columns), ignore_index = True)


# In[111]:


modify_tdf = pd.concat([modify_tag, modify_df], axis=1, join='inner')
modify_tdf


# In[112]:


modify_tdf = modify_tdf.replace(['tag', 'nd','member','relation'],[None, None,None,None])
cols = ['type','id', 'timestamp','uid','user','changeset']
modify_tdf.loc[:,cols] = modify_tdf.loc[:,cols].ffill()
modify_tdf['Mod_type'] = 'modify'
modify_tdf


# In[113]:


frames = [delete_tdf, create_tdf, modify_tdf]
  
result = pd.concat(frames)
result


# In[114]:


neworder = ['Mod_type','type','id','version','timestamp','uid','user','changeset','k','v']
result=result.reindex(columns=neworder)


# In[115]:


result.to_csv('osc2custom.csv', index = False)


# In[ ]:




