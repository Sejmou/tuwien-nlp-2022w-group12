import pandas as pd
import os
import sys

data_dir = "data"
lyric_chunk_folder_path = os.path.join(data_dir, "lyric_chunks")
chunk_paths = [
    os.path.join(lyric_chunk_folder_path, name)
    for name in os.listdir(lyric_chunk_folder_path)
]
chunk_paths.sort()

try:
    chunks = [pd.read_json(path, orient="index") for path in chunk_paths]
except:
    print(
        f"An error occurred while loading lyric chunks from {lyric_chunk_folder_path}. Make sure you've downloaded all data and stored it there."
    )
    sys.exit()

data = pd.concat(chunks).reset_index()

out_path = os.path.join(data_dir, "genius_api_data.json")
data.to_json(
    out_path, orient="index",
)
print(f"Successfully stored data (path: {out_path})")

