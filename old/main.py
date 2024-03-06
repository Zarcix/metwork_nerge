import pandas as pd
import glob
import os
from newsys import run_new_system
from oldsys import run_old_system
import logging

def load_files():
    '''
        Find ./input/*.csv files

            Returns:
                files: all files under "./input/*.csv"
    '''
    # setting the path for joining multiple files
    files = os.path.join("input/", "*.csv")

    # list of merged files returned
    files = glob.glob(files)
    return files

def combine_csv():
    '''
        Load and concat files together

            Returns
                dataframe: dataframe of combined input files
    '''
    files = load_files()
    dataframe1 = pd.read_csv(files[0], sep='comma', header=None, index_col=False, engine="python", quotechar='"')
    # Split without splitting ""
    dataframe1 = dataframe1[0].str.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', expand=True)

    dataframe2 = pd.concat([pd.read_csv(f, sep='comma', header=None, index_col=False, engine="python", quotechar='"').drop([0,1,2,3,4,5,6]) for f in files ])
    # Split without splitting ""
    dataframe2 = dataframe2[0].str.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', expand=True)

    # Combine
    dataframe = pd.concat([dataframe1, dataframe2])

    # Reset the axis for future combining
    dataframe = dataframe.set_axis(range(0, len(dataframe.index)), axis=0, copy = False)

    '''This is for saving to a file if needed'''
    #dataframe.to_csv("out.csv", index=False, sep=',', header=False)

    return dataframe

def layout_type(dataframe):
    '''
        If 6, new layout
        If 11, old layout
    '''
    return len(dataframe.columns) == 6

def main():
    a = lambda x: "INFO" if x is None else x.upper()
    logging.basicConfig(level = a(os.environ.get("LOG")))
    data = combine_csv()
    run_new_system(data) if layout_type(data) else run_old_system(data)

if __name__ == "__main__":
    main()
