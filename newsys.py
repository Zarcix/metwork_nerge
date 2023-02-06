import pandas as pd
import numpy as np
import glob
import os
import xlsxwriter

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

def sort_data(dataframe):
    '''
        !!! Must be run after set_columns !!!

        Sorts data by IP Addr

            Parameters:
                dataframe: dataframe to sort
    '''
    dataframe.sort_values(by=[4], axis=0, inplace=True)

def remove_duplicates(dataframe):
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

def find_useless_data(dataframe):
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
                    5: ip_10_35_228_1_start
                    6: ip_10_35_231_254_end
                    7: ip_end
    '''


    ### These are all indexes
    # Start index
    ip_start = 1

    # Index start and end for 10.15.*.*
    ip_10_15_start = 0
    ip_10_15_end = 0

    # Index start and end for 10.21.[16-23].*
    ip_10_21_16_1_start = 0
    ip_10_21_23_254_end = 0

    # Index start and end for 10.35.[228-231].*
    ip_10_35_228_1_start = 0
    ip_10_35_231_254_end = 0

    # Last index
    ip_end = len(dataframe.index) + 1

    ip_10_15_start = ip_start
    for data in dataframe[4]:
        ip_10_15_start += 1
        if "10.15" in data:
            break

    ip_10_15_end = ip_10_15_start
    for data in dataframe[4].iloc[ip_10_15_start:]:
        if "10.15" not in data:
            break

        ip_10_15_end += 1

    ip_10_21_16_1_start = ip_10_15_end
    for data in dataframe[4].iloc[ip_10_15_end:]:
        ip_10_21_16_1_start += 1
        if "10.21.16" in data:
            break

    ip_10_21_23_254_end = ip_10_21_16_1_start
    for data in dataframe[4].iloc[ip_10_21_16_1_start:]:
        if "10.21.23" in data:
            for data in dataframe[4].iloc[ip_10_21_23_254_end:]:
                if "10.21.23" not in data:
                    break
                ip_10_21_23_254_end += 1
            break

        ip_10_21_23_254_end += 1

    ip_10_35_228_1_start = ip_10_21_23_254_end
    for data in dataframe[4].iloc[ip_10_21_23_254_end:]:
        ip_10_35_228_1_start += 1
        if "10.35.228" in data:
            break

    ip_10_35_231_254_end = ip_10_35_228_1_start
    for data in dataframe[4].iloc[ip_10_35_228_1_start:]:
        if "10.35.231" in data:
            for data in dataframe[4].iloc[ip_10_35_231_254_end:]:
                if "10.35.231" not in data:
                    break
                ip_10_35_231_254_end += 1
            break

        ip_10_35_231_254_end += 1
    return [ip_start, ip_10_15_start, ip_10_15_end, ip_10_21_16_1_start, ip_10_21_23_254_end, ip_10_35_228_1_start, ip_10_35_231_254_end, ip_end]

def set_campus(dataframe, ips):
    '''
        Set campus when values are in range

            Parameters:
                dataframe: dataframe for campus overwrite
                ips: IP list for campus detection

    '''
    for i in range(ips[0], ips[7]):
        if ips[1] - 1 <= i <= ips[2]:
            dataframe.at[i,0] = "Stockton"
        elif ips[3] <= i <= ips[4]:
            dataframe.at[i,0] = "Sacramento"
        elif ips[5] <= i <= ips[6]:
            dataframe.at[i,0] = "San Francisco"

def remove_bad_data(dataframe, ips):
    '''
        Drop unneeded ranges of data

            Parameters:
                dataframe: dataframe for ip removal
                ips: IP list for bad ip removal

            Return:
                dataframe: dataframe with removed data
    '''
    dataframe.drop(range(ips[0],ips[1]), inplace = True)
    dataframe.drop(range(ips[2],ips[3]), inplace = True)
    dataframe.drop(range(ips[4],ips[5]), inplace = True)
    dataframe.drop(range(ips[6],ips[7]), inplace = True)

    dataframe = dataframe.set_axis(range(1, len(dataframe.index) + 1), axis=0, copy = False)
    return dataframe

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
    dataframe.at[2,4] = ips[6] - ips[5]

def setup_data(dataframe):
    '''
        Helper function to set up big data list

            Parameters:
                dataframe: dataframe for setup and data removal

            Returns:
                dataframe: dataframe that got modified
                ips: IP list of useful ips
    '''
    ips = find_useless_data(dataframe)

    set_campus(dataframe, ips)
    dataframe = remove_bad_data(dataframe, ips)

    return (dataframe, ips)

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

def run_new_system(dataframe):
    print("New Layout Detected")
    (dataframe_excel_header, dataframe_excel_body) = set_columns(dataframe)
    dataframe_excel_data = dataframe_excel_body.iloc[1:]
    dataframe_excel_body = dataframe_excel_body.iloc[:1]

    sort_data(dataframe_excel_data)

    dataframe_excel_data = remove_duplicates(dataframe_excel_data)

    (dataframe_excel_data, ips) = setup_data(dataframe_excel_data)
    setup_stats(dataframe_excel_header, ips)

    dataframe = pd.concat([dataframe_excel_header, dataframe_excel_body, dataframe_excel_data], ignore_index = True)
    save_file(dataframe)

if __name__ == "__main__":
    main()
