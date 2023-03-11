import xml.etree.ElementTree as ET
import pandas as pd

def transform_xml(xml_doc):
    attr = xml_doc.attrib
    for xml in xml_doc.findall('.////'):
        dict = attr.copy()
        dict.update(xml.attrib)

        yield dict

etree = ET.parse('jabo.xml')
eroot = etree.getroot()
trans = transform_xml(eroot)
df = pd.DataFrame(list(trans))
df.to_csv('extracted.csv')