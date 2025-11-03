# Douban Movie Dataset

A comprehensive dataset of Chinese movie reviews and information from Douban, one of China's largest movie review platforms.

## Dataset Description

This dataset contains detailed information about movies collected from Douban (豆瓣), including Top 250 movies, high-rating movies, and regional movie collections (Chinese, Western, Japanese, and Hong Kong films). The dataset includes comprehensive metadata such as ratings, cast, directors, plot summaries, genres, release dates, and more.

### Dataset Size

- **All Movies**: 2,065 records (includes additional movies from various sources)
- **Top 250 Movies**: 250 records
- **High Rating Movies**: 480 records
- **Chinese Movies**: 500 records
- **Western Movies**: 500 records
- **Japanese Movies**: 460 records
- **Hong Kong Movies**: 500 records

### Data Collection Date

The data was collected in 2025 using automated web scraping from Douban's public APIs and web pages.

## Data Structure

The dataset is provided in CSV format (Comma-Separated Values), which is compatible with Excel, data analysis tools, and most programming languages.

## Data Fields

Each movie record contains the following fields:

| Field           | Type    | Description                                          |
| --------------- | ------- | ---------------------------------------------------- |
| `movie_id`      | Integer | Unique Douban movie ID                               |
| `title`         | String  | Movie title                                          |
| `rating`        | Float   | Average rating (0-10 scale)                          |
| `total_ratings` | Integer | Total number of ratings                              |
| `directors`     | String  | Director name (first director only)                  |
| `actors`        | String  | Main actors (up to 5 actors, comma-separated)        |
| `screenwriters` | String  | Screenwriter name (first screenwriter only)          |
| `release_date`  | String  | Release date (YYYY-MM-DD format)                     |
| `genres`        | String  | Movie genres (comma-separated)                       |
| `countries`     | String  | Production countries/regions                         |
| `languages`     | String  | Languages used in the movie                          |
| `runtime`       | String  | Movie runtime (e.g., "142 分钟")                     |
| `summary`       | String  | Plot summary/description                             |
| `tags`          | String  | User-generated tags (up to 20 tags, comma-separated) |
| `imdb`          | String  | IMDb link (if available)                             |
| `link`          | String  | Douban movie page URL                                |
| `poster`        | String  | Movie poster image URL                               |

## Files in this Dataset

### Main Collections

- `douban_all_movies.csv` - All movies collection (~2,065 records, includes movies from various sources)

### Rating Collections

- `douban_top250_movies.csv` - Top 250 movies on Douban
- `douban_high_rating.csv` - High-rating movies (~480 records)

### Regional Collections

- `douban_chinese_movies.csv` - Chinese language movies (~500 records)
- `douban_western_movies.csv` - Western movies (~500 records)
- `douban_japanese_movies.csv` - Japanese movies (~460 records)
- `douban_hongkong_movies.csv` - Hong Kong movies (~500 records)

## Usage Examples

### Loading CSV data in Pandas (Python)

```python
import pandas as pd

# Load the complete dataset
df = pd.read_csv('douban_all_movies.csv')

# Display basic information
print(df.head())
print(df.info())
print(f"Total movies: {len(df)}")

# Filter movies with rating > 9.0
high_rated = df[df['rating'] > 9.0]
print(f"Found {len(high_rated)} highly rated movies")
```

### Loading CSV data using Python CSV module

```python
import csv

movies = []
with open('douban_top250_movies.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        movies.append(row)

print(f"Loaded {len(movies)} movies")
print(f"First movie: {movies[0]['title']}")
```

### Loading CSV in R

```r
library(readr)

# Load the dataset
movies <- read_csv('douban_all_movies.csv')

# Display summary
summary(movies)
head(movies)
```

## Potential Use Cases

1. **Recommendation Systems**: Build movie recommendation algorithms based on genres, ratings, and user preferences
2. **Sentiment Analysis**: Analyze plot summaries and user reviews
3. **Market Research**: Study movie trends across different regions and time periods
4. **Content Analysis**: Extract insights about directors, actors, and genre popularity
5. **Cross-platform Analysis**: Compare Douban ratings with other rating platforms
6. **Natural Language Processing**: Train models on movie summaries and descriptions
7. **Data Visualization**: Create dashboards showing movie trends and statistics

## Data Quality Notes

- All data has been deduplicated using `movie_id` as the unique identifier
- Actor lists are limited to the top 5 main actors for consistency
- Director and screenwriter fields contain only the primary (first) person
- Ratings are normalized on a 0-10 scale
- Summary text is in Chinese (Simplified Chinese)

## Data Source

Data collected from Douban (https://movie.douban.com) using public APIs and web scraping. This dataset is provided for educational and research purposes only.

## License

This dataset is provided for educational and research purposes. Please respect Douban's Terms of Service and robots.txt when using this data. Commercial use may require additional permissions.

## Acknowledgments

- Data source: Douban (https://movie.douban.com)
- Dataset compiled using automated collection tools

## Citation

If you use this dataset in your research, please cite:

```
Douban Movie Dataset (2025)
Collection of movie data from Douban platform
```

## Dataset Metadata

- **Version**: 1.0
- **Last Updated**: 2025
- **Format**: CSV (Comma-Separated Values)
- **Encoding**: UTF-8
- **Total Size**: ~15MB (CSV format only)

## Contact

For questions or issues regarding this dataset, please refer to the source repository or create an issue.
