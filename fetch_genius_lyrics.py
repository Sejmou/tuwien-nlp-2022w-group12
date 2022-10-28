import pandas as pd
import numpy as np
from lyricsgenius import Genius
from dotenv import load_dotenv
import os
from argparse import ArgumentParser
import sys
import time
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=True)

data_dir = "data"
data_without_lyrics_path = os.path.join(data_dir, "data_no_lyrics.csv")
try:
    data_without_lyrics = pd.read_csv(data_without_lyrics_path)
except:
    print(
        f"Could not load data for fetching lyrics. Make sure that '{data_without_lyrics_path}' exists!"
    )
    sys.exit()

parser = ArgumentParser()
parser.add_argument(
    "-c",
    "--chunksize",
    dest="chunk_size",
    metavar="chunk size",
    type=int,
    default=100,
    help="the size of the lyrics chunk JSON files that should be created (processing in chunks makes it easier to cancel the lyrics fetching and continue later)",
)
parser.add_argument(
    "-s",
    "--start",
    dest="start_idx",
    metavar="start index",
    type=int,
    default=0,
    help=f"(zero-based) index of first row of '{data_without_lyrics_path}' that the script should fetch lyrics for",
)
parser.add_argument(
    "-e",
    "--end",
    dest="end_idx",
    metavar="end index",
    type=int,
    default=data_without_lyrics.index[-1],
    help=f"(zero-based) index of last row of '{data_without_lyrics_path}' that the script should fetch lyrics for",
)

args = parser.parse_args()
chunk_size = args.chunk_size
start_idx = args.start_idx
end_idx = args.end_idx

if start_idx < 0:
    print("start index must not be smaller than 0!")
    sys.exit()

if end_idx < 1:
    print("end index must not be smaller than 1!")
    sys.exit()

if start_idx > end_idx:
    print("start index must not be larger than end index!")
    sys.exit()

if end_idx > len(data_without_lyrics):
    print(
        f"end index must not be greater than number of rows in '{data_without_lyrics_path}' ({len(data_without_lyrics)})"
    )
    sys.exit()

if chunk_size > len(data_without_lyrics):
    print(
        f"chunk size must not be greater than number of rows in '{data_without_lyrics_path}' ({len(data_without_lyrics)})"
    )
    sys.exit()

# https://stackoverflow.com/a/28882020/13727176
def split_dataframe(df, chunk_size=1000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size : (i + 1) * chunk_size])
    return chunks


chunks = split_dataframe(data_without_lyrics[start_idx:end_idx], chunk_size=chunk_size)

load_dotenv()  # loads GENIUS_ACCESS_TOKEN environment variable from .env
genius = Genius()  # uses GENIUS_ACCESS_TOKEN
genius.verbose = False  # Turn off status messages
genius.remove_section_headers = True  # Remove section headers (e.g. [Chorus])


def get_lyrics(row):
    while True:
        try:
            # this try...except is a workaround for request timeout issues; we just retry the song search until it works
            result = genius.search_song(row.title, row.artist, get_full_info=True)
            if result is not None:
                data = result.to_dict()
                data["msd_tid"] = row.msd_tid
                return pd.Series(
                    data
                ).sort_index()  # not sure if sorting is really necessary, but at least JSON props always have same order than probably?
            break
        except Exception as e:
            pass
            # the print statements are not printed properly (probably because pandarallel also writes to stdout)
            # so, logging errors is pretty useless lol
            # print(
            #    f"Could not fetch lyrics for title '{row.title}' and artist '{row.artist}'"
            # )
            # print(e)
            # print(f"Retrying...")


lyric_chunks_folder_path = os.path.join(data_dir, "lyric_chunks")
if not os.path.exists(lyric_chunks_folder_path):
    os.mkdir(lyric_chunks_folder_path)


for i, chunk in enumerate(chunks):
    print(
        f"processing chunk {i+1} of {len(chunks)} (rows {chunk.index[0]}-{chunk.index[-1]})"
    )
    start_time = time.time()
    lyrics = chunk.parallel_apply(
        get_lyrics, axis=1, result_type="expand"
    )  # using pandarallel speeds things up significantly :)
    lyrics.to_json(
        os.path.join(
            lyric_chunks_folder_path, f"{chunk.index[0]}-{chunk.index[-1]}.json",
        ),
        orient="index",
    )
    print("processed chunk in " + f"{(time.time() - start_time):.2f}" + " seconds")

