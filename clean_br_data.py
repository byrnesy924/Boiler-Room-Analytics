import pandas as pd
import re
from matplotlib import pyplot as plt
from rapidfuzz import ratio

def identify_remix_or_edit(track_name):
    """Superseded by pandas str functions"""
    regex_get_remix = re.compile("\(.*\)")
    regex_remove_edit = re.compile("remix|\(|\)|edit|Remix|Edit|bootleg|Bootleg")
    return regex_get_remix.findall(track_name)

if __name__=="__main__":
    df = pd.read_csv("")  # TODO

    # Split B2B
    df["Artist"] = df["Artist"].str.split(regex="\&|\sand\|\sAnd|B2B|b2b")
    # TODO rename columns as necessary
    # TODO validate no "Ands" are picked up correctly - check uniques that contain and before hand
    # Or duplicate rows for these

    df["RemixOrEdit"] = df["Name"].str.extractall("\(.*\)")
    df["RemixOrEdit"] = df["RemixOrEdit"].str.replace("remix|\(|\)|edit|bootleg", "", case=False)
    # TODO check if a scond col is created by this
    # TODO - do some manual assessment on cleanliness of this
    # TODO check for square brackets - thats a label


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



