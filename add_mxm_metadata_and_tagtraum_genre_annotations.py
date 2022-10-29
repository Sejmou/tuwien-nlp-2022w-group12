import pandas as pd
import os
from io import BytesIO
from zipfile import ZipFile
import requests
import sys


def get_remote_zip(url):
    resp = requests.get(url)
    return ZipFile(BytesIO(resp.content))


def get_remote_textfile(url):
    resp = requests.get(url)
    return resp.text


def fetch_data(url, target_path, is_zip=False):
    if is_zip:
        zip_file = get_remote_zip(url)
        with zip_file.open(zip_file.namelist()[0]) as f:
            content = f.read().decode()
            with open(target_path, "w") as out:
                out.write(content)
    else:
        textfile = get_remote_textfile(url)
        with open(target_path, "w") as out:
            out.write(textfile)


data_dir = "data"

# load mappings of MSD track ID to musiXmatch track ID extracted from musiXmatch dataset SQLite database file (http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db)
# this is a subset of the original full mapping of MSD IDs to musiXmatch IDs (roughly 237k out of 779k)
# tracks were removed if:
# * they were duplicates (i.e. mentioned in the MSD duplicate list (http://millionsongdataset.com/blog/11-3-15-921810-song-dataset-duplicates))
# * they could not be used because of restrictions (including copyright)
# * they were only instrumental tracks
msd_to_mxm_path = os.path.join(data_dir, "msd_to_mxm.csv")
if not os.path.exists(msd_to_mxm_path):
    print(
        "could not load mapping of MSD track IDs to musiXmatch track IDs. Make sure you've stored it in '{msd_to_mxm_path}'!"
    )
    sys.exit()

msd_to_mxm = pd.read_csv(msd_to_mxm_path)
print("loaded MSD to musiXmatch ID mapping extracted from musiXmatch dataset SQLite DB")

# load original full list of mappings of MSD track ID to musiXmatch track ID as well
# while it contains duplicates it might still be useful because it contains additional song metadata
# to obtain additional metadata, we will merge it with the "correct mappings" using the MSD ID
mapping_file_path = os.path.join(data_dir, "mxm_779k_matches.txt")
if not os.path.exists(mapping_file_path):
    print("fetching full mapping with additional metadata")
    fetch_data(
        "http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_779k_matches.txt.zip",
        mapping_file_path,
        True,
    )
mxm_mapping_full = pd.read_table(
    mapping_file_path,
    skiprows=18,
    names=[
        "msd_tid",
        "msd_artist_name",
        "msd_title",
        "mxm_tid",
        "mxm_artist_name",
        "mxm_title",
    ],
    sep="<SEP>",
    engine="python",
)
print("loaded full mapping with additional metadata")

# merge the mappings as mentioned above
merged = pd.merge(msd_to_mxm, mxm_mapping_full, on="msd_tid")
# as msd_to_mxm is essentially a subset of mxm_mapping_full, the columns mxm_tid_x and mxm_tid_y are exactly the same
# we can remove one of them and rename the other
merged = merged.drop(columns=["mxm_tid_y"]).rename(columns={"mxm_tid_x": "mxm_tid"})
print("merged mappings")

# merge with tagtraum genre annotations (https://www.tagtraum.com/msd_genre_datasets.html)
# out of the three available variants, we pick the combined dataset 2 without ambiguous annotations (CD2C)
# the main reason is that it contains only a single genre annotation, which will make training the classifier easier
tagtraum_cd2c_file_path = f"{data_dir}/msd_tagtraum_cd2c.cls"
tagtraum_cd2c_zip_link = f"https://www.tagtraum.com/genres/msd_tagtraum_cd2c.cls.zip"

if not os.path.exists(tagtraum_cd2c_file_path):
    print("fetching genre annotations")
    fetch_data(tagtraum_cd2c_zip_link, tagtraum_cd2c_file_path, True)

tagtraum_cd2c = pd.read_table(
    tagtraum_cd2c_file_path, sep="\t", names=["msd_tid", "genre"], skiprows=7
)
print("loaded genre annotations")


# Now we can merge the musiXmatch data we with the genre annotations
songs_and_genres = pd.merge(merged, tagtraum_cd2c, on="msd_tid")

# for the final dataset we do some more data processing:
# 1. drop the MSD title and artist columns as musiXmatch title/artist info is more suitable for our purposes (no references to specific *version* of a song, e.g. Single/Radio Edit etc.)
# 2. remove the "mxm_" prefix of the remaining musiXmatch title and artist columns
# 3. rearrange the columns
data = songs_and_genres.drop(columns=["msd_artist_name", "msd_title"]).rename(
    columns={"mxm_artist_name": "artist", "mxm_title": "title"}
)[["msd_tid", "mxm_tid", "title", "artist", "genre", "is_test"]]
print("merged track mappings with genre annotations")

# write to csv
out_path = os.path.join(data_dir, "data_no_lyrics.csv")
print(f"writing data to '{out_path}'")
data.to_csv(out_path, index=False)
print("done")

