import pandas as pd
import requests
import json
import time
dfspot=pd.read_csv('spotify.csv', encoding='ISO-8859-1')
def clean_and_convert(df, columns):
    for column in columns:
        if column in df.columns:
            # Check if the column is of object dtype (string-like)
            if df[column].dtype == 'object':
                # Remove commas and convert to numeric
                df[column] = pd.to_numeric(df[column].astype(str).str.replace(',', ''), errors='coerce')
            else:
                # If it's already numeric, just ensure it's the right type
                df[column] = pd.to_numeric(df[column], errors='coerce')

            # Convert to integer, replacing NaN with a default value (e.g., 0)
            df[column] = df[column].fillna(0).astype(int)

    return df

columns_to_convert = [
    "All Time Rank", "Spotify Streams", "Spotify Playlist Count",
    "Spotify Playlist Reach", "YouTube Views",
    "YouTube Likes", "TikTok Posts", "TikTok Likes", "TikTok Views", "AirPlay Spins",
    "SiriusXM Spins", "Deezer Playlist Reach", "Pandora Streams", "Pandora Track Stations",
    "Soundcloud Streams", "Shazam Counts", "Explicit Track", "YouTube Playlist Reach"
] #big int columns failing dbt seed surpassing postgres int
newdfspot = clean_and_convert(dfspot, columns_to_convert)
newdfspot = newdfspot.fillna(0) #replace NaNs
newdfspot.to_csv('cleanedspotify.csv', encoding='utf-8', index=False)

# below fixing corrupt source title,artist and album name encoding data with deezer api, remaining 5 songs fixed
# manually
dfspot=pd.read_csv('cleanedspotify.csv')
def fetch_deezer_data(isrc):
    url = f"https://api.deezer.com/2.0/track/isrc:{isrc}"
    print(url)
    response = requests.get(url)
    print(response)
    textresponse = response.text
    if response.status_code == 200:
        if "no data" in textresponse:
            return None
        else:
            return json.loads(textresponse)
    else:
        return None

def extract_deezer_info(data):
    try:
        title = data['title']
        artist_name = data['artist']['name']
        album_title = data['album']['title']
        return title, artist_name, album_title
    except:
        print(data)

def process_dataframe(df):
    for index, row in df.iterrows():
        if ('Â' in row['Track']) or ('Â' in row['Album Name']) or ('Â' in row['Artist']):
            isrc = row['ISRC']
            deezer_data = fetch_deezer_data(isrc)
            time.sleep(0.5) #api quota fix, around 9 min runtime for this dataset
            if deezer_data:
                # Extract the relevant information from the JSON data
                title, artist_name, album_title = extract_deezer_info(deezer_data)

                # Update the DataFrame with the new data
                df.at[index, 'Track'] = title
                df.at[index, 'Album Name'] = album_title
                df.at[index, 'Artist'] = artist_name

                # You can add more fields here if needed
    return df

# Process the DataFrame
fixeddf = process_dataframe(dfspot)
dfspot.to_csv('cleanedspotifyencodingfix.csv', encoding='utf-8', index=False)