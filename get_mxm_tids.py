# This script extracts musiXmatch track IDs for the Million Song Dataset from the musiXmatch SQLite DB file (http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db)
# of the musiXmatch dataset (http://millionsongdataset.com/musixmatch/)

# The DB has two tables, but only `lyrics` is of interest to us.
# It contains bag-of-words-lyrics for all the songs in the dataset, including the MSD track IDs and the track IDs of the songs on musiXmatch.

# We're only interested in the (MSD and musiXmatch) track IDs, as we don't want only the bag-of-word-lyrics and
# in theory, the musiXmatch track IDs could be used to fetch the full-text lyrics using the musiXmatch API.

# However, it later turned out that we could not obtain the full-text lyrics without paying for the "Plus" plan

# So, we decided to instead fetch lyrics from https://genius.com/ using the Lyrics Genius python package (https://github.com/johnwmillr/LyricsGenius)

# The musiXmatch -> MSD track mappings still help us for the purpose of data collection:

# Obviously, songs for which no lyrics could be obtained are not included, and the researchers that collected the data also removed duplicates mentioned in the official MSD duplicate list (http://millionsongdataset.com/blog/11-3-15-921810-song-dataset-duplicates)
# As Genius.com also has a very good lyrics database we should be able to find lyrics for most of the songs on there as well

import pandas as pd
import sqlite3
import os
import sys

data_dir = "data"
mxm_db_path = os.path.join(data_dir, "mxm_dataset.db")

if not os.path.exists(mxm_db_path):
    print(
        f"Could not find musiXmatch SQLite DB file (path: '{mxm_db_path}'). Make sure you've downloaded it from 'http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db' and stored it!"
    )
    sys.exit()

# get the lyrics table
print("loading lyrics table")
with sqlite3.connect("data/mxm_dataset.db") as con:
    lyrics = pd.read_sql_query("SELECT * FROM lyrics;", con)


# `track_id`,  `mxm_id` and `is_test` are the only relevant columns for our purposes.
# For convenience, we also rename `track_id` to `msd_tid`.
relevant = (
    lyrics[["track_id", "mxm_tid", "is_test"]]
    .drop_duplicates()
    .rename(columns={"track_id": "msd_tid"})
)
print("removed irrelevant columns")

data_dir = "data"
if not os.path.exists(data_dir):
    os.mkdir(data_dir)

out_path = os.path.join(data_dir, "msd_to_mxm.csv")

print(f"writing data to '{out_path}'")
relevant.to_csv(out_path, index=False)
print("done")
