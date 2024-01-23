from typing import Tuple
import pandas as pd
import logging, sys
from datamod import DataMod

# (Starting IP, Ending IP, Campus)

brain = DataMod()

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



def save_file(dataframe: pd.DataFrame):
    '''
        Save the modified excel sheet

            Parameters:
                dataframe: dataframe to write to save file
    '''
    outFile = 'output.xlsx'
    with pd.ExcelWriter(outFile)  as writer:
        logging.debug(f"Saving results to {outFile}")
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
    (df_header, df_column_names) = set_columns(dataframe)
    df_data = df_column_names.iloc[1:]
    df_column_names = df_column_names.iloc[:1]

    (df_data, ips) = brain.process_data(df_data)
    setup_stats(df_header, ips)

    logging.debug("Concat-ing all dataframes")
    dataframe = pd.concat([df_header, df_column_names, df_data], ignore_index = True)
    save_file(dataframe)