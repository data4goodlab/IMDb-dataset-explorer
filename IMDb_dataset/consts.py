import os



IMDB_NAMES_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"
IMDB_TITLES_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_CREW_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"
IMDB_RATING_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
IMDB_PRINCIPALS_URL = "https://datasets.imdbws.com/title.principals.tsv.gz"

BASE_DIR_NAME = ".imdb"

BASEPATH = os.path.expanduser(os.path.join('~', BASE_DIR_NAME))


OUTPUT_PATH = f"{BASEPATH}/output"


def set_output_path(output_path):
    global OUTPUT_PATH
    OUTPUT_PATH = output_path
