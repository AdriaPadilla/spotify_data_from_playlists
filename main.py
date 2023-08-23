import spotipy
import pandas as pd
import json
import glob
from tqdm import tqdm
import numpy as np
import os

from spotipy.oauth2 import SpotifyClientCredentials
import time # Per afegir pauses

SPOTIPY_CLIENT_ID = ''
SPOTIPY_CLIENT_SECRET = ''

sources = pd.read_excel("")

## FOLDERS TO SAVE JSON DATA
project_name = ""
main_folder = f"data/{project_name}"
artists_json_folder = f"{main_folder}/artists_data"
playlist_items_json_folder = f"{main_folder}/playlist_items"
track_feateures_json_folder = f"{main_folder}/track_features"


folders = [main_folder,artists_json_folder,playlist_items_json_folder,track_feateures_json_folder]

for folder in folders:
    if not os.path.isdir(folder):
        os.mkdir(folder)

auth_manager = SpotifyClientCredentials(SPOTIPY_CLIENT_ID,SPOTIPY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)
loop = 0
o = 0 # Dummie Offset

def get_playlist_items(data, loop, o):
    playlist_id = data["playlist_id"]
    print(f"{loop} for {playlist_id} with offset: {o}")
    query = sp.playlist_items(playlist_id, fields=None, limit=100, offset=o, market=None)
    query["adria_data"] = data
    with open(f'{playlist_items_json_folder}/{data["playlist_id"]}-{loop}.json', 'w', encoding='utf-8') as f:
        json.dump(query, f, ensure_ascii=False, indent=4)
    if query["next"] != None:
        o = o + 100
        loop = loop + 1
        time.sleep(2)
        get_playlist_items(data, loop, o)
    else:
        pass

def recap_all_tracks():
    playlists = glob.glob(f"{playlist_items_json_folder}/*.json")
    print(playlists)

    general_track_frame = []

    for p in tqdm(playlists):
        with open(p) as file:
            parsed_json = json.load(file)
            items = parsed_json["items"]
            for item in items:
                try:
                    track_data = {}
                    track_data["track_name"] = item["track"]["name"]
                    track_data["track_id"] = item["track"]["id"]
                    track_data["track_popularity"] = item["track"]["popularity"]
                    track_data["track_duration"] = item["track"]["duration_ms"]
                    track_data["artist"] = item["track"]["artists"][0]["name"]
                    track_data["artist_id"] = item["track"]["artists"][0]["id"]
                    track_data["album_name"] = item["track"]["album"]["name"]
                    track_data["album_release_date"] = item["track"]["album"]["release_date"]
                    track_data["url_artist"] = item["track"]["artists"][0]["external_urls"]["spotify"]
                    track_data["url_track"] = item["track"]["external_urls"]["spotify"]
                    track_data["from_playlist_id"] = parsed_json["adria_data"]["playlist_id"]
                    track_data["from_playlist_name"] = parsed_json["adria_data"]["playlist_name"]

                    track_frame = pd.DataFrame.from_dict([track_data], orient="columns")
                    general_track_frame.append(track_frame)
                except (KeyError, TypeError):
                    print("KeyError")
                    pass

    final_track_frame = pd.concat(general_track_frame)
    final_track_frame.to_csv(f"{main_folder}/dataset-1-tracks.csv", index=False, sep="\t", quotechar='"')

def get_audio_features():
    tracks = pd.read_csv(f"{main_folder}/dataset-1-tracks.csv", sep="\t", quotechar='"')
    id_list = tracks["track_id"].to_list()
    id_list = list(set(id_list))
    q = len(id_list)
    chunks = q/50
    list_group = np.array_split(id_list, chunks)
    ch_number = 0
    for d in tqdm(list_group):
        features = sp.audio_features(tracks=d)
        with open(f'{track_feateures_json_folder}/tracks_features-{ch_number}.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=4)
        ch_number = ch_number+1

        time.sleep(2)

def merge_data():
    main_dataset = pd.read_csv(f"{main_folder}/dataset-1-tracks.csv", sep="\t", quotechar='"')
    features_files = glob.glob(f"{track_feateures_json_folder}/*.json")
    general_feature_dataframe = []

    for f in tqdm(features_files):
        with open(f) as file:
            parsed_json = json.load(file)
            for track in parsed_json:
                try:
                    features_df = pd.DataFrame.from_dict([track], orient="columns")
                    features_df["track_id"] = features_df["id"]
                    features_df.drop(["type","id","uri","track_href","analysis_url", "duration_ms"], axis=1, inplace=True)

                    general_feature_dataframe.append(features_df)
                except KeyError:
                    print(f"Keyerror on {track}")
                    pass

    final_features_frame = pd.concat(general_feature_dataframe)
    final_features_frame.drop_duplicates(inplace=True)
    print(final_features_frame)
    merged_frame = pd.merge(main_dataset, final_features_frame, on="track_id")
    merged_frame.to_excel(f"{main_folder}/dataset-2-audio_features.xlsx", index=False)

def get_genres():
    df = pd.read_excel(f"{main_folder}/dataset-2-audio_features.xlsx")
    id_list = df["artist_id"].to_list()
    id_list = list(set(id_list))

    max_ids_inquery = 48
    chunks = round(len(id_list)/max_ids_inquery)
    groups =  np.array_split(id_list, chunks)
    loop = 0
    for g in tqdm(groups):
        query = sp.artists(g)
        with open(f'{artists_json_folder}/artists_data-{loop}.json', 'w', encoding='utf-8') as f:
            json.dump(query, f, ensure_ascii=False, indent=4)
        loop = loop+1
        time.sleep(2)

def add_genres_to_dataset():
    main_dataset = pd.read_excel(f"{main_folder}/dataset-2-audio_features.xlsx")
    artists_files = glob.glob(f"{artists_json_folder}/*.json")
    general_artist_frame_list = []
    for f in tqdm(artists_files):
        with open(f) as file:
            parsed_json = json.load(file)
            for artist in parsed_json["artists"]:
                artist_data = {}
                artist_data["followers"] = artist["followers"]["total"]
                artist_data["popularity"] = artist["popularity"]
                try:
                    artist_data["genres"] = artist["genres"][0]
                except IndexError:
                    artist_data["genres"] = "no data"

                artist_data["artist_id"] = artist["id"]
                general_artist_frame_list.append(pd.DataFrame.from_dict([artist_data], orient="columns"))

    artists_genres = pd.concat(general_artist_frame_list)
    merged_frame = pd.merge(main_dataset, artists_genres, on="artist_id")
    merged_frame.to_excel(f"{main_folder}/dataset-3-complete-data.xlsx", index=False)

def get_playlist_info():
    main_dataset = pd.read_excel(f"{main_folder}/dataset-3-complete-data.xlsx")
    playlists_id = main_dataset["from_playlist_id"].to_list()

    playlists_id = list(set(playlists_id))
    playlist_info_list = []

    for p in tqdm(playlists_id):
        playlist_info = sp.playlist(p)
        playlist_followers = playlist_info["followers"]["total"]
        d = {"from_playlist_id": [p], "playlist_followers":[playlist_followers]}
        df = pd.DataFrame(data=d)
        playlist_info_list.append(df)
        time.sleep(2)

    final = pd.concat(playlist_info_list)
    merged_frame = pd.merge(main_dataset, final, on="from_playlist_id")
    merged_frame.to_excel(f"{main_folder}/final_dataset.xlsx", index=False)

for index, row in sources.iterrows():
    data = {} # diccionari per guardar tot
    data["playlist_id"] = row["Id"]
    data["playlist_name"] = row["Nombre"]
    get_playlist_items(data, loop, o)

recap_all_tracks()
get_audio_features()
merge_data()
get_genres()
add_genres_to_dataset()
get_playlist_info()
