from typing import Tuple
import pandas as pd
import logging

class DataMod:
    ips_to_keep: list[Tuple[str, str, str]] = []
    
    def load_ip_list(self):
        self.ips_to_keep = [
            ("10.15", "10.15", "Stockton"), 
            ("10.21.16", "10.21.23", "Sacramento"), 
            ("10.35.216", "10.35.223", "San Francisco"),
            ("10.35.228", "10.35.231", "San Francisco")
        ]
    
    def __init__(self):
        # TODO Load ip range from a file
        self.load_ip_list()
        pass
    
    def process_data(self, dataframe: pd.DataFrame) -> Tuple[pd.DataFrame, list[Tuple[int, int, str]]]:
        '''
        Wrapper Function for sort, remove, and setup
        
        Parameters
            @dataframe: dataframe containing data
        
        Returns
            @dataframe: dataframe containing sorted and filtered data of ips to keep
            @ips: IP list of useful ips
        '''
        df_data = self.sort_data(dataframe)
        df_data = self.remove_duplicates(df_data)
        return self.setup_data(df_data)
    
    def setup_data(self, dataframe: pd.DataFrame) -> Tuple[pd.DataFrame, list[Tuple[int, int, str]]]:
        '''
            Clean up data and sets up data

                Parameters:
                    @dataframe: dataframe with extra data

                Returns:
                    @dataframe: dataframe containing all wanted data
                    @ips: IP list of useful ips
        '''
        ips = self._find_useful_data_indices(dataframe)

        dataframe = self._set_campus(dataframe, ips)
        dataframe = self._clean_data(dataframe, ips)

        return (dataframe, ips)

    def sort_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        logging.debug("Sorting data by IPs")
        return dataframe.sort_values(by=[4], axis=0, inplace=False)

    def remove_duplicates(self, df_with_dups: pd.DataFrame) -> pd.DataFrame:
        logging.debug("Removing duplicate IPs")
        df_without_dups = df_with_dups.drop_duplicates(subset=[2], inplace = False)
        if df_without_dups is not None:
            df_with_dups = df_without_dups.set_axis(range(1, len(df_without_dups.index) + 1), axis=0, copy = False)
        
        return df_with_dups

    def _find_useful_data_indices(self, dataframe: pd.DataFrame) -> list[Tuple[int, int, str]]:
        ip_ranges: list[Tuple[int, int, str]] = []

        for ip in self.ips_to_keep:
            result = self._get_useless_indexes(dataframe[4], ip[0], ip[1])
            ip_ranges.append((result[0], result[1], ip[2]))

        return ip_ranges

    def _set_campus(self, dataframe: pd.DataFrame, ips: list[Tuple[int, int, str]]) -> pd.DataFrame:
        '''
            Set campus when values are in range

                Parameters:
                    dataframe: dataframe for setting campus for ips
                    ips: list of ips with ranges and campus

        '''
        for ip_range in ips:
            for i in range(ip_range[0], ip_range[1] + 1):
                dataframe.at[i,0] = ip_range[2]
        
        return dataframe
    
    def _clean_data(self, dataframe: pd.DataFrame, ips: list[Tuple[int, int, str]]) -> pd.DataFrame:
        '''
            Drop unneeded ranges of data

                Parameters:
                    dataframe: dataframe for ip retrieval
                    ips: wanted ip indexes

                Return:
                    dataframe: dataframe with wanted data
        '''
        retDataframe = pd.DataFrame()
        
        # Loop over all ip ranges and append ips in range
        for ip_range in ips:
            retDataframe = pd.concat([retDataframe, dataframe.iloc[ip_range[0]:ip_range[1]]])

        # Reset the axis
        dataframe = retDataframe.set_axis(range(1, len(retDataframe.index) + 1), axis=0, copy = False)
        return dataframe

    def _get_useless_indexes(self, dataframe: pd.DataFrame, start_ip: str, end_ip: str) -> Tuple[int, int]:
        # Set starting index
        ip_start = 0
        for data in dataframe:
            ip_start += 1
            if start_ip in data:
                break
        
        # Set ending index
        ip_end = ip_start
        for data in dataframe.iloc[ip_start:]:
            if end_ip in data:
                # Go to the end of the ip range
                for data in dataframe.iloc[ip_end:]:
                    if end_ip not in data:
                        break
                    ip_end += 1
                break
            ip_end += 1
        
        logging.debug(f"{start_ip} -> {end_ip}: {ip_start} {ip_end}")
        return (ip_start, ip_end)