from typing import Tuple
import pandas as pd
import logging, sys

def set_columns(dataframe_full: pd.DataFrame):
    '''
        Remove unneeded columns

            Parameters:
                dataframe_excel: dataframe to modify columns

            Returns:
                dataframe header: dataframe's header after modification
                dataframe body: dataframe's body after modification
    '''
    # Split header from excel file
    dataframe_excel_header: pd.DataFrame = dataframe_full.iloc[:6]

    # Remove unnecessary columns and reindex them
    dataframe_excel_header = dataframe_excel_header.drop([3,6,8,9,10], axis=1)
    dataframe_excel_header.reset_index(drop=True, inplace = True)

    # Get body from full file
    dataframe_excel_body: pd.DataFrame = dataframe_full.iloc[6:]

    # Remove headers that aren't needed from body
    dataframe_excel_body = dataframe_excel_body.drop([3,6,8,9,10], axis=1)

    # Insert Campus column
    dataframe_excel_body.insert(0, "0", "")
    dataframe_excel_body.at[6,"0"] = "Campus"

    # Reset axis for both header and body
    dataframe_excel_header = dataframe_excel_header.set_axis(range(0, 6), axis=1, copy = False)
    dataframe_excel_body = dataframe_excel_body.set_axis(range(0, 7), axis=1, copy = False)

    return (dataframe_excel_header, dataframe_excel_body)

def sort_data(dataframe: pd.DataFrame):
    '''
        !!! Must be run after set_columns !!!

        Sorts data by IP Addr

            Parameters:
                dataframe: dataframe to sort
    '''
    dataframe.sort_values(by=[4], axis=0, inplace=True)

def remove_duplicates(dataframe: pd.DataFrame):
    '''
        !!! Must be run after set_columns !!!
        Removes duplicates on Users

            Parameters:
                dataframe: input dataframe to remove duplicates

            Returns:
                dataframe: modified dataframe

    '''
    dataframe.drop_duplicates(subset=[2], inplace = True)
    dataframe = dataframe.set_axis(range(1, len(dataframe.index) + 1), axis=0, copy = False)
    return dataframe

def find_useful_data_indices(dataframe: pd.DataFrame) -> list[Tuple[int, int, str]]:
    '''
        Indexes through data and finds the useless ip's and grabs index

            Parameters:
                dataframe: Dataframe to find useless data in

            Returns:
                int[]: array of indexes in following order:
                    0: ip_start
                    1: ip_10_15_start
                    2: ip_10_15_end
                    3: ip_10_21_16_1_start
                    4: ip_10_21_23_254_end
                    5: ip_10_35_216_1_start
                    6: ip_10_35_223_254_end
                    7: ip_10_35_228_1_start
                    8: ip_10_35_231_254_end
                    9: ip_end
    '''


    # (Starting IP, Ending IP, Campus)
    ips_to_keep: list[Tuple[str, str, str]] = [
        ("10.15", "10.15", "Stockton"), 
        ("10.21.16", "10.21.23", "Sacramento"), 
        ("10.35.216", "10.35.223", "San Francisco"), 
        ("10.35.228", "10.35.231", "San Francisco")
    ]

    ip_ranges: list[Tuple[int, int, str]] = []

    for ip in ips_to_keep:
        result = get_useless_indexes(dataframe[4], ip[0], ip[1])
        ip_ranges.append((result[0], result[1], ip[2]))

    return ip_ranges

def get_useless_indexes(dataframe: pd.DataFrame, start_ip: str, end_ip: str):
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
    
def set_campus(dataframe: pd.DataFrame, ips: list[Tuple[int, int, str]]):
    
    '''
        Set campus when values are in range

            Parameters:
                dataframe: dataframe for campus overwrite
                ips: IP list for campus detection

    '''
    for ip_range in ips:
        for i in range(ip_range[0], ip_range[1] + 1):
            dataframe.at[i,0] = ip_range[2]

def clean_data(dataframe: pd.DataFrame, ips: list[Tuple[int, int, str]]):
    '''
        Drop unneeded ranges of data

            Parameters:
                dataframe: dataframe for ip removal
                ips: IP list for bad ip removal

            Return:
                dataframe: dataframe with removed data
    '''
    retDataframe = pd.DataFrame()
    for ip_range in ips:
        retDataframe = pd.concat([retDataframe, dataframe.iloc[ip_range[0]:ip_range[1]]])

    dataframe = retDataframe.set_axis(range(1, len(retDataframe.index) + 1), axis=0, copy = False)
    return dataframe

def setup_stats(dataframe: pd.DataFrame, ips: list[Tuple[int, int, str]]):
    '''
        Set up statistics in the header

            Parameters:
                dataframe: dataframe for insertion
                ips: IP list for stats calculation
    '''
    # Create lists with only their respective campus
    stk_ips = filter(lambda x: x[2] == "Stockton", ips) 
    sac_ips = filter(lambda x: x[2] == "Sacramento", ips)
    sf_ips = filter(lambda x: x[2] == "San Francisco", ips)

    # Set them based on campus
    dataframe.at[1,2] = "Stockton"
    dataframe.at[2,2] = sum(map(lambda x: x[1] - x[0], stk_ips))

    dataframe.at[1,3] = "Sacramento"
    dataframe.at[2,3] = sum(map(lambda x: x[1] - x[0], sac_ips))

    dataframe.at[1,4] = "San Francisco"
    dataframe.at[2,4] = sum(map(lambda x: x[1] - x[0], sf_ips))

def setup_data(dataframe: pd.DataFrame):
    '''
        Helper function to set up big data list

            Parameters:
                dataframe: dataframe for setup and data removal

            Returns:
                dataframe: dataframe that got modified
                ips: IP list of useful ips
    '''
    ips = find_useful_data_indices(dataframe)

    set_campus(dataframe, ips)
    dataframe = clean_data(dataframe, ips)

    return (dataframe, ips)

def save_file(dataframe: pd.DataFrame):
    '''
        Save the modified excel sheet

            Parameters:
                dataframe: dataframe to write to save file
    '''
    with pd.ExcelWriter('output.xlsx')  as writer:
        dataframe.to_excel(
            writer,
            sheet_name='Data',
            columns = None,
            header = False,
            index = False,
            freeze_panes=(7, 0),
        )
        # Formatting Save
        for column in dataframe:
            column_width = max(dataframe[column].astype(str).map(len).max(), len(str(column)))
            col_idx = dataframe.columns.get_loc(column)
            writer.sheets['Data'].set_column(col_idx, col_idx, column_width)

def run_old_system(dataframe: pd.DataFrame):
    logging.debug("Old Layout Detected")
    (dataframe_excel_header, dataframe_excel_body) = set_columns(dataframe)
    dataframe_excel_data = dataframe_excel_body.iloc[1:]
    dataframe_excel_body = dataframe_excel_body.iloc[:1]

    sort_data(dataframe_excel_data)

    dataframe_excel_data = remove_duplicates(dataframe_excel_data)

    (dataframe_excel_data, ips) = setup_data(dataframe_excel_data)
    setup_stats(dataframe_excel_header, ips)

    dataframe = pd.concat([dataframe_excel_header, dataframe_excel_body, dataframe_excel_data], ignore_index = True)
    save_file(dataframe)
