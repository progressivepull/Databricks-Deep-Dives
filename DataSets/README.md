# Last.fm Global Trends — Dataset

Top artists, tracks, and tags from the [Last.fm API](https://www.last.fm/api), covering every supported country plus global charts.

## Files

| File | Description |
|---|---|
| `trends.db` | DuckDB database (all tables) |
| `parquet/geo_top_artists.parquet` | Top artists per country (Parquet) |
| `parquet/geo_top_tracks.parquet` | Top tracks per country (Parquet) |
| `parquet/global_top_artists.parquet` | Global top artists (Parquet) |
| `parquet/global_top_tracks.parquet` | Global top tracks (Parquet) |
| `parquet/global_top_tags.parquet` | Global top tags (Parquet) |
| `csv/geo_top_artists.csv` | Top artists per country (CSV) |
| `csv/geo_top_tracks.csv` | Top tracks per country (CSV) |
| `csv/global_top_artists.csv` | Global top artists (CSV) |
| `csv/global_top_tracks.csv` | Global top tracks (CSV) |
| `csv/global_top_tags.csv` | Global top tags (CSV) |

## Tables & Columns

### `geo_top_artists` — Top artists per country

| Column | Type | Description |
|---|---|---|
| `country` | string | Country display name (e.g. `France`, `Türkiye`) |
| `rank` | int | Chart position (1 = most listeners) |
| `artist` | string | Artist name |
| `artist_mbid` | string | MusicBrainz ID for the artist (may be empty) |
| `artist_url` | string | Last.fm URL for the artist page |
| `listeners` | int | Listener count in that country |
| `fetched_at` | timestamp | When this row was last written to the DB |

### `geo_top_tracks` — Top tracks per country

| Column | Type | Description |
|---|---|---|
| `country` | string | Country display name |
| `rank` | int | Chart position (1 = most listeners) |
| `track` | string | Track title |
| `track_url` | string | Last.fm URL for the track page |
| `artist` | string | Artist name |
| `artist_mbid` | string | MusicBrainz ID for the artist (may be empty) |
| `artist_url` | string | Last.fm URL for the artist page |
| `listeners` | int | Listener count in that country |
| `playcount` | int | Play count in that country |
| `fetched_at` | timestamp | When this row was last written to the DB |

### `global_top_artists` — Global top artists

| Column | Type | Description |
|---|---|---|
| `rank` | int | Chart position (1 = most listeners globally) |
| `artist` | string | Artist name |
| `artist_mbid` | string | MusicBrainz ID for the artist (may be empty) |
| `artist_url` | string | Last.fm URL for the artist page |
| `listeners` | int | Total global listener count |
| `playcount` | int | Total global play count |
| `fetched_at` | timestamp | When this row was last written to the DB |

### `global_top_tracks` — Global top tracks

| Column | Type | Description |
|---|---|---|
| `rank` | int | Chart position (1 = most played globally) |
| `track` | string | Track title |
| `track_mbid` | string | MusicBrainz ID for the track (may be empty) |
| `track_url` | string | Last.fm URL for the track page |
| `artist` | string | Artist name |
| `artist_mbid` | string | MusicBrainz ID for the artist (may be empty) |
| `artist_url` | string | Last.fm URL for the artist page |
| `listeners` | int | Total global listener count |
| `playcount` | int | Total global play count |
| `fetched_at` | timestamp | When this row was last written to the DB |

### `global_top_tags` — Global top tags

Top 10,000 tags by usage. The Last.fm API reports ~2.8 M total tags; this table captures the most popular ones.

| Column | Type | Description |
|---|---|---|
| `rank` | int | Chart position (1 = most used) |
| `tag` | string | Tag name (e.g. `rock`, `electronic`, `seen live`) |
| `tag_url` | string | Last.fm URL for the tag page |
| `reach` | int | Number of distinct users who applied this tag |
| `taggings` | int | Total number of times this tag has been applied |
| `fetched_at` | timestamp | When this row was last written to the DB |

## Usage

### DuckDB

```python
import duckdb

con = duckdb.connect("trends.db", read_only=True)

# Top 10 artists in Brazil
con.execute("SELECT rank, artist, listeners, artist_url FROM geo_top_artists WHERE country = 'Brazil' ORDER BY rank LIMIT 10").df()

# Countries where an artist appears in the top 50
con.execute("SELECT country, rank, listeners FROM geo_top_artists WHERE artist = 'Taylor Swift' AND rank <= 50 ORDER BY rank").df()

# Global top 20 tracks with links
con.execute("SELECT rank, track, artist, playcount, track_url FROM global_top_tracks ORDER BY rank LIMIT 20").df()

# Top 20 tags by reach
con.execute("SELECT rank, tag, reach, taggings FROM global_top_tags ORDER BY rank LIMIT 20").df()

con.close()
```

### pandas

```python
import pandas as pd

df = pd.read_parquet("parquet/geo_top_artists.parquet")
df = pd.read_csv("csv/global_top_tags.csv")
```

## Update frequency

Data is refreshed weekly. Each run skips tables whose data is less than 7 days old and whose row count matches the API.
