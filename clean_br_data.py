import numpy as np
import pandas as pd
import re
from matplotlib import pyplot as plt
from rapidfuzz import fuzz
from time import perf_counter
# TODO - remove "Unreleased" from names and artists


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


if __name__ == "__main__":
    df = pd.read_csv(r"Data\1001_tracklist_set_lists.csv")
    raw_df = df.copy()

    # Remove artist from the track name - regex remove everything before "\-"
    df["TrackName"] = df["TrackName"].str.replace(r".*\-\s", "", regex=True)
    df["TrackName"] = df["TrackName"].str.strip()
    df["TrackName"] = df["TrackName"].str.replace(r'\s+', ' ', regex=True)
    # From here we don't really care about the track name except to search beatport.

    # String process of artists
    # Some are clearlly encoding errors - I have caught a few and manually adjusted them below
    # ?-Ziq --> Mu-Zic; ?Ztek -> AZtek; ?kkord --> Akkord; Ã˜ [Phase] --> Ø [Phase]; --> Rødhåd; Ã‚me --> Ame; Ã…re:gone --> Åre:gone
    # Ã…MRTÃœM --> ÅMRTÜM; Ã…NTÃ†GÃ˜NIST --> ÅNTÆGØNIST
    # May not be necessary...
    print(pd.unique(df["Artist"].sort_values()))
    manual_artists_to_clean = [("?-Ziq", "Mu-Ziq"), ("?Ztek", "AZtek"), ("?kkord", "Akkord"), ("Ã˜ [Phase]", "Ø [Phase]"),
                               ("RÃ¸dhÃ¥d", "Rødhåd"), ("Ã‚me" "Ame"), ("Ã…re:gone", "Åre:gone"),
                               ("Ã…MRTÃœM", "ÅMRTÜM"), ("Ã…NTÃ†GÃ˜NIST", "ÅNTÆGØNIST"), ("ÃŽÂ¼-Ziq", "Mu-Ziq"), ("Î¼-Ziq", "Mu-Ziq")]
    for artist_tup in manual_artists_to_clean:
        df["Artist"] = df["Artist"].str.replace(artist_tup[0], artist_tup[1])
        df["DJ"] = df["DJ"].str.replace(artist_tup[0], artist_tup[1])

    # Split out the artist column for colabs
    tokenized_artists = df["Artist"].str.split(r"\s\&\s", expand=True, regex=True)
    tokenized_artists.columns = [f"Artist{i}" for i in range(len(tokenized_artists.columns))]
    for col in tokenized_artists.columns:
        tokenized_artists[col] = tokenized_artists[col].str.strip()
        tokenized_artists[col] = tokenized_artists[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(tokenized_artists, left_index=True, right_index=True)

    # Split B2B
    b2b = df["DJ"].str.split(r"\s\&\s|\sand\s\|\sAnd\s|\sB2B\s|\sb2b\s", expand=True, regex=True)
    b2b.columns = [f"DJ{i}" for i in range(len(b2b.columns))]
    for col in b2b.columns:
        b2b[col] = b2b[col].str.strip()
        b2b[col] = b2b[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(b2b, left_index=True, right_index=True)

    df["RemixOrEdit"] = df["TrackName"].str.extract(r"\((.*?)\)", expand=False)
    df["RemixOrEdit"] = df["RemixOrEdit"].str.replace(
        "(Remix)|(\()|(\))|(Edit)|(bootleg)|(Version)|(Vocal Mix)|(Mix)|(Instrumental Mix)|(Instrumental)|(Dub)",
        "",
        case=False,
        regex=True
    )
    df["RemixOrEdit"] = df["RemixOrEdit"].str.strip()
    multi_remix = df["RemixOrEdit"].str.split(r"\s\&\s|\sand\s|\sAnd\s", expand=True, regex=True)
    multi_remix.columns = [f"RemixOrEdit{i}" for i in range(len(multi_remix.columns))]
    for col in multi_remix.columns:
        multi_remix[col] = multi_remix[col].str.strip()
        multi_remix[col] = multi_remix[col].str.replace(r"\s+", " ", regex=True)
    df = df.merge(multi_remix, left_index=True, right_index=True)

    for col in df:
        if col == "Number":
            continue
        df[col] = df[col].str.replace("\?(?!ME)|(^ID$)|(^Id$)", "", regex=True)  # Replace all "?" and ID/Id except for ?ME, that's a legit artist

    print(regression_check_cleaning(df_start=raw_df, df_end=df))
    df.to_csv(r"Data\\preprocessed_br_data.csv", encoding="utf-8")

    # TODO
    artist_cols = df.filter(regex=r"(DJ\d+)|(RemixOrEdit\d+)|(Artist\d+)")
    all_artist_cols = df.filter(regex=r"(DJ\d*)|(RemixOrEdit\d*)|(Artist\d*)").columns

    start = perf_counter()
    list_of_artists: np.ndarray = pd.unique(artist_cols.stack())
    # Approach for cleaning artist names:
    # This is a static, point in time database. It is enough to get a similarity score for all pairs of artists
    # then to evaluate a cutoff point of similarity score and merge all above the threshold
    # Manually choosing this score prevents false positives/negatives but lacks the ability to be automated in the future
    # If ever repeating the task if downloading data, this can be repeated
    # and hopefully because the data is taken Beatport (what I currently suspect) there shouldnt be too many issues (except maybe remixes...)

    # Numpy Cartesian product of artists
    cartesian_sorted = np.sort(np.array(np.meshgrid(list_of_artists, list_of_artists)).T.reshape(-1, 2), axis=1)
    intermediary_df = pd.DataFrame(
        cartesian_sorted,
        columns=["Artist1", "Artist2"]
    )
    # convert to df - Combinations column is tuples of Arist 1 and Artist 2. Note it is a sorted tuple
    intermediary_df["Combinations"] = intermediary_df.apply(lambda x: (x["Artist1"], x["Artist2"]), axis=1)
    # remove duplicates (halves dataframe size)
    product_of_artists = pd.DataFrame(pd.unique(intermediary_df["Combinations"]).tolist(), columns=["Artist1", "Artist2"])

    # remove exact pairs
    product_of_artists = product_of_artists.loc[product_of_artists["Artist1"] != product_of_artists["Artist2"], :]  # remove exact matches
    # calc string similarity of all pairs of artists
    product_of_artists["StringSimilarity"] = product_of_artists.apply(lambda x: fuzz.ratio(x["Artist1"], x["Artist2"]), axis=1)
    threshold = 80
    merge_artist = product_of_artists.loc[product_of_artists["StringSimilarity"] > threshold, :]

    print(f"Took {(perf_counter() - start)/60}mins to do string similarity.")
    fig, ax = plt.subplots(figsize=(40, 40))
    ax.axvline(threshold, color='red', linestyle='--', linewidth=2)
    ax.set_title("Histogram of Values")
    ax.set_yscale('log')  # to try elucidate the taile
    ax.set_xlabel("Value")
    ax.set_ylabel("Frequency")
    product_of_artists["StringSimilarity"].hist(ax=ax, bins=30)  # would be nice to show the threshold line
    plt.show()
    fig.savefig("histogram_of_similarity_values.png")

    fig_tail, ax_tail = plt.subplots(figsize=(40, 40))
    ax.axvline(threshold, color='red', linestyle='--', linewidth=2)
    ax_tail.set_title("Tail of Histogram of Values")
    ax_tail.set_yscale('log')  # to try elucidate the taile
    ax_tail.set_xlabel("Value")
    ax_tail.set_ylabel("Frequency")
    product_of_artists.loc[product_of_artists["StringSimilarity"] > 65, "StringSimilarity"].hist(ax=ax_tail, bins=30)  # would be nice to show the threshold line
    plt.show()
    fig_tail.savefig("tail_of_histogram.png")

    product_of_artists.loc[product_of_artists["StringSimilarity"] > 65, :].to_csv(r"Data\\FuzzyMatching.csv")

    # Then apply merges onto datasets
    final_df = df.copy()
    list_of_pairs = merge_artist[["Artist1", "Artist2"]].to_dict("split", index=False)["data"]
    dict_of_pairs = {item[1]: item[0] for item in list_of_pairs}
    for col in artist_cols.columns:
        final_df[col] = final_df[col].map(lambda x: dict_of_pairs[x] if x in dict_of_pairs.keys() else x)

    print(f"Took {(perf_counter() - start)/60}mins to do all of artist fuzzy matching.")
    final_df.to_csv(r"Data\cleaned_boiler_room_data.csv", encoding="utf-8", index=False)  # TODO think about parquet
    final_df.to_parquet(r"Data\cleaned_boiler_room_data.parquet", index=False)
    # number of missing genres - whether it is worth getting these from the beatport API
    print(df.isnull().sum())

    # TODO at the end regression check anything that had data and now is empty


