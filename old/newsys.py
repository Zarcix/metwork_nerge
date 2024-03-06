from typing import Tuple
import pandas as pd
import logging
from datamod import DataMod

def set_columns(dataframe_excel):
    '''
        Remove unneeded columns

            Parameters:
                dataframe_excel: dataframe to modify columns

            Returns:
                dataframe header: dataframe's header after modification
                dataframe body: dataframe's body after modification
    '''
    # Split header from excel file
    dataframe_excel_header = dataframe_excel.iloc[:6]

    dataframe_excel_header.reset_index(drop=True, inplace = True)


    # Split body from excel file
    dataframe_excel_body = dataframe_excel.iloc[6:]

    # Insert Campus column
    dataframe_excel_body.insert(0, "0", pd.NA)
    dataframe_excel_body.at[6,"0"] = "Campus"

    # Reset axis for both header and body
    dataframe_excel_header = dataframe_excel_header.set_axis(range(0, 6), axis=1, copy = False)
    dataframe_excel_body = dataframe_excel_body.set_axis(range(0, 7), axis=1, copy = False)

    return (dataframe_excel_header, dataframe_excel_body)

def setup_stats(dataframe, ips):
    '''
        Set up statistics in the header

            Parameters:
                dataframe: dataframe for insertion
                ips: IP list for stats calculation
    '''
    dataframe.at[1,2] = "Stockton"
    dataframe.at[1,3] = "Sacramento"
    dataframe.at[1,4] = "San Francisco"

    dataframe.at[2,2] = ips[2] - ips[1]
    dataframe.at[2,3] = ips[4] - ips[3]

    sf_total = ips[6] - ips[5]
    sf_total += ips[8] - ips[7]
    dataframe.at[2,4] = sf_total

def save_file(dataframe):
    '''
        Save the modified excel sheet

            Parameters:
                dataframe: dataframe to write to save file
    '''
    writer = pd.ExcelWriter('output.xlsx')

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
    writer.close()

def run_new_system(dataframe: pd.DataFrame):
    logging.debug("New Layout Detected")
    print("WARNING: THIS IS UNTESTED. USE AT YOUR OWN RISK")
    
    brain = DataMod()
    
    (df_header, df_column_names) = set_columns(dataframe)
    df_data = df_column_names.iloc[1:]
    df_column_names = df_column_names.iloc[:1]
    
    (df_data, ips) = brain.process_data(df_data)
    setup_stats(df_header, ips)

    logging.debug("Concat-ing all dataframes")
    dataframe = pd.concat([df_header, df_column_names, df_data], ignore_index = True)
    save_file(dataframe)
