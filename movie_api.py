from pydantic import BaseModel, BeforeValidator, ValidationError
from typing import Annotated, List, Dict
import mysql.connector
from dotenv import dotenv_values
import requests

TMDB_BEARER_TOKEN, TMDB_API_KEY, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = dotenv_values(".env").values()

base_url = "https://api.themoviedb.org/3/movie"
headers = {
    "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
    "Content-Type": "application/json"
}

def truncate_to_255(value: str) -> str:
    return value[:255] if value else ""

ShortOverview = Annotated[str, BeforeValidator(truncate_to_255)]

class Movie(BaseModel):
    tmdb_id: int
    poster_path: str
    title: str
    overview: ShortOverview
    release_date: str
    vote_average: float
    # keywords: str

def extract_movie_data():
    api_response = []
    try: 
        api_response = requests.get(f"{base_url}/popular", headers=headers)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    finally:
        return api_response.json()
    
def transform_movie_data(raw: List[Dict]) -> List[Movie]:
    validated_movies = []
    for item in raw:
        
        try:
            movie = Movie(
                tmdb_id=item['id'],
                poster_path=item['poster_path'],
                title=item['title'],
                overview=item['overview'],
                release_date=item['release_date'],
                vote_average=item['vote_average'],
                # keywords=item.get('keywords').apply(json.dumps)
            )
            validated_movies.append(movie)
        except ValidationError as e:
            print(f"Validation error: {e}")
    return validated_movies
    
def load_movies(movies: List[Movie]):
    connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    try:
        with connection.cursor() as cursor:
            query = """
                INSERT INTO movies (tmdb_id, poster_path, title, overview, release_date, vote_average)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    poster_path = VALUES(poster_path),
                    title = VALUES(title),
                    overview = VALUES(overview),
                    release_date = VALUES(release_date),
                    vote_average = VALUES(vote_average)
                    # keywords = VALUES(keywords)
                """
            data = [(m.tmdb_id, m.poster_path, m.title, m.overview, m.release_date, m.vote_average) for m in movies]
            cursor.executemany(query, data)
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows into the database.")
    except mysql.connector.Error as e:
        print(f"Something went wrong: {e}")
    finally:
        connection.close()

pop_movies = extract_movie_data()
# print(f"movies: {pop_movies}")
transformed_movies = transform_movie_data(pop_movies['results'])
load_movies(transformed_movies)
