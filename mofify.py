import pandas as pd

# Load the CSV file
input_file = 'output.csv'  # Replace with your actual file name
output_file = 'output_modify.csv'  # Desired output file name

# Read the CSV file
df = pd.read_csv(input_file)

# Rename the columns (example: renaming to 'Column1', 'Column2', ...)
new_column_names = ['TrackID', 'TrackName', 'TrackNumber', 'Duration', 'Explicit', 'AlbumName', 'Artists', 'Genres', 'AvailableMarkets','Popularity', 'Acoustiness', 'Energy', 'Instrumentalness', 'Liveness', 'Loudness', 'Mode', 'Speechiness', 'Tempo', 'TimeSignature', 'Valence']  # Adjust to your desired column names
df.columns = new_column_names

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file, index=False)

print(f"Renamed columns and saved the updated file to '{output_file}'.")
