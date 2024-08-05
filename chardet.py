import pandas as pd
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