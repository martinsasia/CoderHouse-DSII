import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time
import string

# Spotify API credentials
SPOTIPY_CLIENT_ID = 'e83bd9d42fb94d8a83fc0b287001f745'
SPOTIPY_CLIENT_SECRET = '996af9f2ffb9434eaa01c6cd253c1ce0'

# Set up Spotify API client
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID,
                                                           client_secret=SPOTIPY_CLIENT_SECRET),
                     requests_timeout=10, retries=10)

def get_tracks_by_year(year):
    """Fetch tracks from the specified year using Spotify API."""
    all_tracks = []
    queries = [f"{c}*" for c in string.ascii_lowercase]  # Create queries a*, b*, c*, etc.

    for query in queries:
        print(f"Fetching tracks for query '{query}' in year {year}...")
        results = sp.search(q=f'{query} year:{year}', type='track', limit=50)
        tracks = results['tracks']['items']
        total = results['tracks']['total']
        fetched = len(tracks)
        offset = 50

        # Handle pagination
        while fetched < min(total, 1000):  # API limit of 1000 results per query
            time.sleep(0.1)  # Avoid hitting rate limits
            results = sp.search(q=f'{query} year:{year}', type='track', limit=50, offset=offset)
            tracks.extend(results['tracks']['items'])
            fetched += len(results['tracks']['items'])
            offset += 50
            if len(results['tracks']['items']) == 0:
                break

        all_tracks.extend(tracks)
        time.sleep(0.1)  # Short delay to respect rate limits

    return all_tracks

def extract_track_info(track):
    """Extract relevant information from a track."""
    # Get audio features
    audio_features = sp.audio_features(track['id'])[0] if track['id'] else {}
    # Get genres (simplified to the first artist)
    artist_info = sp.artist(track['artists'][0]['id']) if track['artists'] else {}
    genres = artist_info.get('genres', [])

    return {
        'id': track.get('id'),
        'name': track.get('name'),
        'artists': ', '.join(artist['name'] for artist in track.get('artists', [])),
        'duration_ms': track.get('duration_ms'),
        'release_date': track['album'].get('release_date') if track.get('album') else None,
        'year': track['album'].get('release_date', '')[:4] if track.get('album') else None,
        'acousticness': audio_features.get('acousticness'),
        'danceability': audio_features.get('danceability'),
        'energy': audio_features.get('energy'),
        'instrumentalness': audio_features.get('instrumentalness'),
        'liveness': audio_features.get('liveness'),
        'loudness': audio_features.get('loudness'),
        'speechiness': audio_features.get('speechiness'),
        'tempo': audio_features.get('tempo'),
        'valence': audio_features.get('valence'),
        'mode': audio_features.get('mode'),
        'key': audio_features.get('key'),
        'popularity': track.get('popularity'),
        'explicit': track.get('explicit'),
        'genre': ', '.join(genres)
    }

def create_dataframe(year):
    """Create a DataFrame with track information for the specified year."""
    all_tracks = get_tracks_by_year(year)
    track_info_list = []
    total_tracks = len(all_tracks)
    print(f"Total tracks fetched for year {year}: {total_tracks}")

    for idx, track in enumerate(all_tracks):
        if track.get('id'):
            track_info = extract_track_info(track)
            track_info_list.append(track_info)
            # Optional: print progress
            if (idx + 1) % 100 == 0:
                print(f"Processed {idx + 1}/{total_tracks} tracks")
        time.sleep(0.01)  # Small delay to respect rate limits

    df = pd.DataFrame(track_info_list)
    return df

# Specify the year you want to test
year_to_test = 2020  # Cambia esto al año que desees probar

# Create DataFrame for the specified year
df = create_dataframe(year_to_test)

# Save DataFrame to CSV
df.to_csv(f'spotify_tracks_{year_to_test}.csv', index=False)
print(f"Dataset saved to spotify_tracks_{year_to_test}.csv")