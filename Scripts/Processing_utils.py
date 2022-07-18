import os
import datetime
import pandas as pd
import xml.etree.ElementTree as ET


class file_processing():
        def __init__(self) -> None:
            self.limit = limit
            pass
        
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

        def save_parquet(self, df, path):
            try:        
                df.to_parquet(path)
            except:
                return 0
            return 1
            

        def posts_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def comments_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def Users_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def Badges_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def postLinks_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def tags_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def votes_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df

        def postHistory_processing(self, path):
            df = self.__get_df_from_xml(path)
            ## Any PreProcessing workflow
            return df
