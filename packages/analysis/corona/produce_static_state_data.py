import os
import json

import pandas as pd

# set variable names
DEMOGRAPHICS_DIR = "data/CovidData/sbamt/production_input"
DEMOGRAPHICS_FN = "demographics.csv"

MERGED_DIR = "data/CovidData/corona/analysis_output"
MERGED_FN = "states_static"

# load data
def load_file(dir: str, fname: str) -> pd.DataFrame:
    """ load google data """

    fpath = os.path.join(dir, fname)
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"Input file does not exist: {fpath}")

    return pd.read_csv(fpath)

def prep_demo(df: pd.DataFrame) -> pd.DataFrame:
    """ Prepare demographics data for merge """

    # drop date column
    drop_cols = ["date"]
    df.drop(drop_cols, axis=1, inplace=True)

    # reshape table
    df = reshape_demo(df)

    return df

def reshape_demo(df: pd.DataFrame) -> pd.DataFrame:
    """ Reshape dataframe """

    df = pd.pivot_table(df, values="value", index="state", columns=["gender", "age_group"])
    df.columns = df.columns.to_flat_index()

    # formatting
    df.columns = ["pop_" + "_".join(col) for col in df.columns]
    df.reset_index(inplace=True)

    return df

def store_data(df: pd.DataFrame, dir: str, fname: str):
    """ Save data """

    if not os.path.exists(dir):
        print(f"Directory for storing result does not exist: {dir}. Trying to create ...")
        os.makedirs(dir)

    fpath = os.path.join(dir, fname)

    # store as csv
    store_csv(df, fpath)

    # store as json
    store_json(df, fpath)

def store_csv(df: pd.DataFrame, fpath: str):
    """ store as csv """
    # save as csv
    fpath = fpath + ".csv"
    df.to_csv(fpath, index=False)

def store_json(df: pd.DataFrame, fpath: str):
    """ store as json """

    df_json = df.to_dict(orient="records")

    for entry in df_json:
        for (key, value) in entry.items():
            if ~pd.notnull(value) == -1:
                entry[key] = 0
            else:
                entry[key] = value

    fpath = fpath + ".json"
    with open(fpath, "w") as fp:
        json.dump(df_json, fp, indent=4)

def run_merge() -> pd.DataFrame:
    """ Run entire merging script """

    df_demo = load_file(DEMOGRAPHICS_DIR, DEMOGRAPHICS_FN)
    df_demo = prep_demo(df_demo)

    store_data(df_demo, MERGED_DIR, MERGED_FN)

    return df_demo

if __name__ == "__main__":
    df = run_merge()
