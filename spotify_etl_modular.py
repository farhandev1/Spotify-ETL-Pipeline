import requests
import pandas as pd
from sqlalchemy import create_engine
import base64

# Function to generate Spotify API token
def get_spotify_token(client_id, client_secret):
    """
    Generates a Spotify API access token using client ID and client secret.
    """
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch token: {response.json()}")

    token = response.json().get("access_token")
    print("Access token generated successfully!")
    return token

# Function to extract Spotify data
def extract_spotify_data(playlist_id, token):
    """
    Extracts data from the Spotify API for a given playlist.
    """
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"API call failed with status code {response.status_code}")
    
    data = response.json()
    tracks = []
    for item in data['items']:
        track = item['track']
        tracks.append({
            'TrackName': track['name'],
            'Artist': ', '.join([artist['name'] for artist in track['artists']]),
            'Album': track['album']['name'],
            'ReleaseDate': track['album']['release_date'],
            'Popularity': track['popularity']
        })
    
    return pd.DataFrame(tracks)

# Function to transform data
def transform_data(raw_data):
    """
    Cleans and transforms the raw Spotify data.
    """
    raw_data['ReleaseDate'] = pd.to_datetime(raw_data['ReleaseDate'], errors='coerce')
    raw_data['Popularity'] = pd.to_numeric(raw_data['Popularity'], errors='coerce')
    cleaned_data = raw_data.drop_duplicates(subset=['TrackName', 'Artist']).dropna()
    return cleaned_data

# Function to load data into Azure SQL
def load_to_azure_sql(cleaned_data, server, database, username, password):
    """
    Loads the cleaned data into Azure SQL Database.
    """
    driver = 'ODBC Driver 18 for SQL Server'
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
    engine = create_engine(connection_string)
    table_name = 'RapPlaylist'
    try:
        cleaned_data.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully loaded into table '{table_name}'!")
    except Exception as e:
        print(f"Error loading data: {e}")

# Master function to run the ETL pipeline
def run_pipeline(client_id, client_secret, playlist_id, sql_config):
    """
    Executes the Spotify ETL pipeline: Extract, Transform, Load.
    """
    print("Starting ETL Pipeline...")

    # Generate API token
    print("Generating Spotify API token...")
    token = get_spotify_token(client_id, client_secret)

    # Extraction
    print("Extracting data...")
    raw_data = extract_spotify_data(playlist_id, token)
    print(f"Extracted {len(raw_data)} rows of data.")

    # Transformation
    print("Transforming data...")
    transformed_data = transform_data(raw_data)
    print(f"Transformed data has {len(transformed_data)} rows.")

    # Loading
    print("Loading data to Azure SQL...")
    load_to_azure_sql(
        transformed_data,
        sql_config['server'],
        sql_config['database'],
        sql_config['username'],
        sql_config['password']
    )

    print("ETL Pipeline completed successfully!")

# Main execution
if __name__ == "__main__":
    # Spotify API credentials
    client_id = "2203ef17b67c4cd08affbf355b164b0b"
    client_secret = "8d5ccac168e146dfafe817e5b1e05fd0"

    # Spotify playlist details
    playlist_id = "2O6rARZJi8IoXLiUdXZPJ3"

    # Azure SQL configuration
    sql_config = {
        "server": "spotify-sql-server.database.windows.net",
        "database": "SpotifyDB",
        "username": "SQL_server",
        "password": "Tajmahal1"
    }

    # Run the pipeline
    try:
        run_pipeline(client_id, client_secret, playlist_id, sql_config)
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
