import os
import json

import pandas as pd

# set variable names
DEMOGRAPHICS_DIR = "data/CovidData/sbamt/production_input"
DEMOGRAPHICS_FN = "demographics.csv"
POPULATIONS_FN = "states_population.csv"
MEDICAL_FN = "medical_distribution.csv"

INHABITANT_STATISTIC_DIR = "data/CovidData/iddw/production_input"
DEATHS_FN = "death_cases.csv"
INCOME_FN = "inhabitant_income.csv"
EMPLOYRD_FN = "total_employed.csv"
UNEMPLOYRD_FN = "total_unemployed.csv"

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

def validate_state_order(origin: pd.Series, mergie: pd.Series):
    if not (origin == mergie).all():
        raise Exception("state oder is incompatible")

def merge_state_population(origin: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
    
    population = population[population.state != 'Deutschland']
    validate_state_order(origin['state'], population['state'])

    new_df = origin.assign(territory_size=population['area'],
                           person_per_km_square=population['per_km_square'])

    return new_df

def merge_medical(origin: pd.DataFrame, medical: pd.DataFrame) -> pd.DataFrame:
    
    medical = medical[medical.state != 'Deutschland']
    validate_state_order(origin['state'], medical['state'])

    new_df = origin.assign(physicians=medical['physicians'],
                           dentists=medical['dentists'],
                           hospitals=medical['hospitals'],
                           beds=medical['beds'])

    return new_df

def merge_deaths(origin: pd.DataFrame, deaths: pd.DataFrame, year: str) -> pd.DataFrame:

    deaths = deaths[deaths.Bundesland != 'Deutschland']
    validate_state_order(origin['state'], deaths['Bundesland'])

    new_df = origin.assign(deaths=deaths[year])

    return new_df

def merge_income(origin: pd.DataFrame, income: pd.DataFrame, year: str) -> pd.DataFrame:

    income = income[income.Bundesland != 'Deutschland']
    validate_state_order(origin['state'], income['Bundesland'])

    new_df = origin.assign(income=income[year])

    return new_df

def merge_employed(origin: pd.DataFrame, employed: pd.DataFrame, year: str) -> pd.DataFrame:

    employed = employed[employed.Bundesland != 'Deutschland']
    validate_state_order(origin['state'], employed['Bundesland'])

    new_df = origin.assign(employed=employed[year])

    return new_df

def merge_unemployed(origin: pd.DataFrame, unemployed: pd.DataFrame, year: str) -> pd.DataFrame:

    unemployed = unemployed[unemployed.Bundesland != 'Deutschland']
    validate_state_order(origin['state'], unemployed['Bundesland'])

    new_df = origin.assign(unemployed=unemployed[year])

    return new_df


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

    population = load_file(DEMOGRAPHICS_DIR, POPULATIONS_FN)
    medical = load_file(DEMOGRAPHICS_DIR, MEDICAL_FN)
    deaths = load_file(INHABITANT_STATISTIC_DIR, DEATHS_FN)
    income = load_file(INHABITANT_STATISTIC_DIR, INCOME_FN)
    employed = load_file(INHABITANT_STATISTIC_DIR, EMPLOYRD_FN)
    unemployed = load_file(INHABITANT_STATISTIC_DIR, UNEMPLOYRD_FN)

    df = merge_state_population(df_demo, population)
    df = merge_medical(df, medical)
    df = merge_deaths(df, deaths, "2018")
    df = merge_income(df, income, "2017")
    df = merge_employed(df, employed, "2018")
    df = merge_unemployed(df, unemployed, "2018")

    store_data(df, MERGED_DIR, MERGED_FN)

    return df

if __name__ == "__main__":
    df = run_merge()
