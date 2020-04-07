import os
import json

import pandas as pd

# set variable names
GOOGLE_DIR = "data/google/production_input"
GOOGLE_FN = "international_local_area_trends_UK_DE_FR_ES_IT_US_SE.csv"

MPOST_DIR = "data/mpost/production_input"
MPOST_FN = "history.v4.csv"

MERGED_DIR = "data/corona/analysis_output"
MERGED_FN = "merged_data"

# load data
def load_file(dir: str, fname: str) -> pd.DataFrame:
    """ load google data """

    fpath = os.path.join(dir, fname)
    if not os.path.exists(fpath):
        raise FileNotFoundError(f"Input file does not exist: {fpath}")

    return pd.read_csv(fpath)

def prep_google(df: pd.DataFrame) -> pd.DataFrame:
    """ make sure data is consistent """

    state_dictionary = {
        "Bavaria": "Bayern",
        "Hesse": "Hessen",
        "Lower Saxony": "Niedersachsen",
        "North Rhine-Westphalia": "Nordrhein-Westfalen",
        "Rhineland-Palatinate": "Rheinland-Pfalz",
        "Saxony": "Sachsen",
        "Saxony-Anhalt": "Sachsen-Anhalt",
        "Thuringia": "Thüringen"
    }
    df["state"] = df["state"].apply(lambda x: state_dictionary[x] if x in list(state_dictionary) else x)

    return df

def merge_google_mpost(df_mpost: pd.DataFrame, df_google: pd.DataFrame) -> pd.DataFrame:
    """ merge both datasets """

    df_full = pd.concat([df_mpost, df_google])
    df_full.reset_index(inplace=True, drop=True)

    return df_full

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
    df["id"] = df["state"] + "_" + df["date"]
    df = df.pivot(index="id", columns='key', values='value').reset_index()
    df["state"] = df["id"].apply(lambda x: x.split("_")[0])
    df["date"] = df["id"].apply(lambda x: x.split("_")[1])
    df.drop("id", axis=1, inplace=True)
    df_json = df.to_dict(orient="records")

    fpath = fpath + ".json"
    with open(fpath, "w") as fp:
        json.dump(df_json, fp, indent=4)


def run_merge() -> pd.DataFrame:

    # load data
    df_mpost = load_file(MPOST_DIR, MPOST_FN)
    df_google = load_file(GOOGLE_DIR, GOOGLE_FN)

    # prep google data
    df_google = prep_google(df_google)

    # merge data
    df_full = merge_google_mpost(df_mpost, df_google)

    # store data
    store_data(df_full, MERGED_DIR, MERGED_FN)

    return df_full

if __name__ == "__main__":
    df = run_merge()