import pandas as pd
import re
from matplotlib import pyplot as plt
from rapidfuzz import ratio


def identify_remix_or_edit(track_name):
    """Superseded by pandas str functions"""
    regex_get_remix = re.compile("\(.*\)")
    regex_remove_edit = re.compile("remix|\(|\)|edit|Remix|Edit|bootleg|Bootleg")
    return regex_get_remix.findall(track_name)


def regression_check_cleaning(df_start: pd.DataFrame, df_end: pd.DataFrame):
    """Finds values that were wiped from the dataframe for human assessment"""
    df_end = df_end[df_start.columns]  # only check matching columns
    condition = df_start.notnull() & df_end.isnull()
    return df_start.loc[condition.any(axis=1), :]


if __name__=="__main__":
    df = pd.read_csv("1001_tracklist_set_lists.csv")
    raw_df = df.copy()

    # Remove artist from the track name - regex remove everything before "\-"
    df["TrackName"] = df["TrackName"].str.replace(r".*\-\s", "", regex=True)
    df["TrackName"] = df["TrackName"].str.strip()
    df["TrackName"] = df["TrackName"].str.replace(r'\s+', ' ', regex=True)
    # From here we don't really care about the track name except to search beatport.

    # String process of artists
    # ?-Ziq --> Mu-Zic; ?Ztek -> AZtek; ?kkord --> Akkord; Ã˜ [Phase] --> Ø [Phase]; --> Rødhåd; Ã‚me --> Ame; Ã…re:gone --> Åre:gone
    # Ã…MRTÃœM --> ÅMRTÜM; Ã…NTÃ†GÃ˜NIST --> ÅNTÆGØNIST
    manual_artists_to_clean = [("?-Ziq", "Mu-Ziq"), ("?Ztek", "AZtek"), ("?kkord", "Akkord"), ("Ã˜ [Phase]", "Ø [Phase]"),
                               ("RÃ¸dhÃ¥d", "Rødhåd"), ("Ã‚me" "Ame"), ("Ã…re:gone", "Åre:gone"),
                               ("Ã…MRTÃœM", "ÅMRTÜM"), ("Ã…NTÃ†GÃ˜NIST", "ÅNTÆGØNIST"), ("ÃŽÂ¼-Ziq", "Mu-Ziq")]
    for artist_tup in manual_artists_to_clean:
        df["Artist"] = df["Artist"].str.replace(artist_tup[0], artist_tup[1])
        df["DJ"] = df["DJ"].str.replace(artist_tup[0], artist_tup[1])

    # Split out the artist column for colabs
    tokenized_artists = df["Artist"].str.split(r"\s\&\s", expand=True, regex=True)
    tokenized_artists.columns = [f"Artist{i}" for i in len(tokenized_artists.columns)]
    for col in tokenized_artists.columns:
        tokenized_artists[col] = tokenized_artists[col].str.strip()
        tokenized_artists[col] = tokenized_artists[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(tokenized_artists)

    # Split B2B
    b2b = df["DJ"].str.split(r"\s\&\s|\sand\s\|\sAnd\s|\sB2B\s|\sb2b\s", expand=True, regex=True)
    b2b.columns = [f"DJ{i}" for i in len(b2b.columns)]
    for col in b2b.columns:
        b2b[col] = b2b[col].str.strip()
        b2b[col] = b2b[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(b2b)

    df["RemixOrEdit"] = df["TrackName"].str.extractall("\(.*\)")
    df["RemixOrEdit"] = df["RemixOrEdit"].str.replace("remix|\(|\)|edit|bootleg|Version", "", case=False)
    multi_remix = df["RemixOrEdit"].str.split(r"\s\&\s|\sand\s|\sAnd\s", expand=True, regex=True)
    multi_remix.columns = [f"Artist{i}" for i in len(tokenized_artists.columns)]
    for col in multi_remix.columns:
        multi_remix[col] = multi_remix[col].str.strip()
        multi_remix[col] = multi_remix[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(multi_remix)
    
    for col in df:
        df[col] = df[col].str.replace("\?(?!ME)", "", regex=True)  # Replace all "?" except for ?ME, that's a legit artist

    print(regression_check_cleaning(df_start=raw_df, df_end=df))

    # TODO
    list_of_artists = pd.unique(pd.concat(df["Artist"], df["Artist2"]), df["DJ"], df["RemixOrEdit"])  # TODO change Artist 2 to second col for B2B
   
    # Approach for cleaning artist names:
    # This is a static, point in time database. It is enough to get a similarity score for all pairs of artists
    # then to evaluate a cutoff point of similarity score and merge all above the threshold
    # Manually choosing this score prevents false positives/negatives but lacks the ability to be automated in the future
    # If ever repeating the task if downloading data, this can be repeated
    # and hopefully because the data is taken Beatport (what I currently suspect) there shouldnt be too many issues (except maybe remixes...)

    # Numpy Cartesian join of
    product_of_artists = list_of_artists.merge(list_of_artists, how="cross")  # TODO check column names
    product_of_artists = product_of_artists.where(product_of_artists["Artist"] != product_of_artists["Artist2"])  # remove exact matches
    product_of_artists["StringSimilarity"] = product_of_artists.apply(lambda x: ratio(x["Artist"], x["Artist2"]))
    threshold = 85
    merge_artist = product_of_artists.where(product_of_artists["StringSimilarity"] > threshold)
    
    fig, ax = plt.subplots(figsize=(20, 20))
    ax.axvline(threshold, color='red', linestyle='--', linewidth=2)
    ax.set_title("Histogram of Values")
    ax.set_xlabel("Value")
    ax.set_ylabel("Frequency")
    product_of_artists["StringSimilarity"].hist(ax=ax)  # would be nice to show the threshold line
    plt.show()

    # Then apply merges onto datasets

    # number of missing genres - whether it is worth getting these from the beatport API
    print(df.isnull().sum())

    # TODO at the end regression check anything that had data and now is empty


