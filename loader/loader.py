import json
import os

def load_secrets() -> dict[str, str]:
    """
    Returns the following fields from secrets.json:
        "host",
        "user",
        "password",
        "database"
    """
    # check if secrets.json exists
    if not os.path.exists('secrets/secrets.json'):
        print('Need to create secrets.json file')
        raise FileNotFoundError('secrets.json not found')
    with open('secrets/secrets.json') as f:
        data = json.load(f)
    return data

def write_secrets(data: dict[str, str]):
    with open('secrets/secrets.json', 'w') as f:
        json.dump(data, f, indent=4)

def download_tsv(url: str, filename: str):
    download_zip(url, filename)
    unzip_tsv(filename)

def download_zip(url: str, filename: str):
    os.system(f'wget {url} -O {filename}')

def unzip_tsv(filename: str):
    os.system(f'gunzip {filename}')

def download_data():
    urls = ["https://datasets.imdbws.com/name.basics.tsv.gz", 
            "https://datasets.imdbws.com/title.akas.tsv.gz", 
            "https://datasets.imdbws.com/title.basics.tsv.gz", 
            "https://datasets.imdbws.com/title.crew.tsv.gz", 
            "https://datasets.imdbws.com/title.episode.tsv.gz", 
            "https://datasets.imdbws.com/title.principals.tsv.gz", 
            "https://datasets.imdbws.com/title.ratings.tsv.gz"
            ]
    filenames = ['data/' + url.split('/')[-1] for url in urls]
    for url, filename in zip(urls, filenames):
        download_tsv(url, filename)

def main():
    download_data()

if __name__ == '__main__':
    main()