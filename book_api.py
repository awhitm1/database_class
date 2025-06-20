from dotenv import dotenv_values
import requests


TMDB_BEARER_TOKEN, TMDB_API_KEY, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = dotenv_values(".env").values()


base_book_url = "https://openlibrary.org/search.json?q=test"
headers = {
    "User-Agent": "MyAppName/1.0 (myemail@example.com)"
}


response = requests.get(base_book_url, headers=headers)

