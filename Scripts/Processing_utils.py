import os
import datetime
import pandas as pd
import xml.etree.ElementTree as ET


class file_processing():
        def __init__(self, limit = None) -> None:
            self.limit = limit
            pass

        def __get_df_from_xml_v2(self, path: str): #Stable starting from Pandas 1.3.2
            df = pd.read_xml(path)
            return df
        
        def __get_df_from_xml(self, path: str):
            loop_count = 0
            tree = ET.parse(path)
            root = tree.getroot()
            #print('{} : {}'.format(path, len(root)))
            first = True 
            for child in root:
                if first:
                    columns_list = list(child.attrib.keys())
                    #print(columns_list)
                    df = pd.DataFrame(columns=columns_list)
                    print('{} Dataframe created'.format(path))
                    first = False
                if loop_count == self.limit:
                    break
                row_as_dict = child.attrib
                df = df.append(row_as_dict, ignore_index=True)
                loop_count += 1
            return df

        def __datetime_formating(self, time: str) -> datetime.datetime:
            datetime_formated = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f')
            return datetime_formated    

        def save_parquet(self, df, file_name, dir=None):
            try:        
                df.to_parquet(os.path.join(dir, '{}.parquet'.format(file_name)))
            except:
                return 0
            return 1
            

        def posts_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            df.CreationDate = df.CreationDate.apply(self.__datetime_formating)
            df.LastEditDate = df.LastEditDate.apply(self.__datetime_formating)
            df.LastActivityDate = df.LastActivityDate.apply(self.__datetime_formating)
            ## Any PreProcessing workflow
            return df 

        def comments_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            df.CreationDate = df.CreationDate.apply(self.__datetime_formating)
            ## Any PreProcessing workflow
            return df

        def users_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df

        def badges_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df

        def postLinks_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df

        def tags_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df

        def votes_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df

        def postHistory_processing(self, path):
            df = self.__get_df_from_xml_v2(path)
            df.columns = df.columns.str.lower()
            ## Any PreProcessing workflow
            return df
