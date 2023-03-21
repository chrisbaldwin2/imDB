import asyncio
from db import MySQLDatabase, Database
from loader import load_secrets
import argparse

commit_frequency = 1000

def clean_split_line(line: str) -> list[str]:
        split_line = line.strip('\n').split('\t')
        return split_line

def tsv_to_sql_name(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    def line_to_data(line: str, columns: list[str]) -> dict:
        # print(columns, line)
        split_line = clean_split_line(line)
        name_data = dict(zip(columns[:4], split_line[:4]))
        for k in list(name_data.keys()):
            if name_data[k] == '\\N':
                del name_data[k]
        profession_data = dict([(columns[0], split_line[0]), (columns[4], split_line[4])])
        titles_data = dict([(columns[0], split_line[0]), (columns[5], split_line[5])])
        # print(data)
        return name_data, profession_data, titles_data
    # Make DDL columns for name_basics
    columns = [
        'nconst VARCHAR(255) PRIMARY KEY',
        'primaryName VARCHAR(255)',
        'birthYear INT',
        'deathYear INT',
    ]
    # Create table
    db.add_table('name_basics', columns)
    # Make DDL columns for relation profession
    columns = [
        'nconst VARCHAR(255)',
        'primaryProfession VARCHAR(255)',
    ]
    # Create table
    db.add_table('relation_profession', columns)
    # Make DDL columns for relation titles
    columns = [
        'nconst VARCHAR(255)',
        'tconst VARCHAR(255)',
    ]
    # Create table
    db.add_table('relation_titles', columns)
    with open(filename, 'r') as file:
        # Read column headers 
        columns = clean_split_line(file.readline())
        print('Loading name_basics')
        # Go through each line and insert into table
        for i, line in enumerate(file):
            if i >= num_lines:
                break
            print(f'  Processing line {i+1} / {num_lines}', end='\r')
            name, professions, titles = line_to_data(line, columns)
            db.insert('name_basics', name, commit=(i%commit_frequency == 0))
            for j, profession in enumerate(professions['primaryProfession'].split(',')):
                if profession == '\\N':
                    continue
                db.insert('relation_profession', {'nconst': name['nconst'], 'primaryProfession': profession}, commit=(i%commit_frequency == 0 and j==0))
            for j, title in enumerate(titles['knownForTitles'].split(',')):
                if title == '\\N':
                    continue
                db.insert('relation_titles', {'nconst': name['nconst'], 'tconst': title}, commit=(i%commit_frequency == 0 and j==0))
        db.commit()
        print('\nDone loading name_basics')

def tsv_to_sql_title(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    def line_to_data(line: str, columns: list[str]) -> dict:
        # print(columns, line)
        split_line = clean_split_line(line)
        title_data = dict(zip(columns[:8], split_line[:8]))
        for k in list(title_data.keys()):
            if title_data[k] == '\\N':
                del title_data[k]
                continue
            if len(title_data[k]) > 254 and (k == 'primaryTitle' or k == 'originalTitle'):
                title_data[k] = title_data[k][:255]
        # print(data)
        genre_data = dict([(columns[0], split_line[0]), (columns[8], split_line[8])])
        return title_data, genre_data
    # Make DDL columns for title_basics
    columns = [
        'tconst VARCHAR(255) PRIMARY KEY',
        'titleType VARCHAR(255)',
        'primaryTitle VARCHAR(255)',
        'originalTitle VARCHAR(255)',
        'isAdult INT',
        'startYear INT',
        'endYear INT',
        'runtimeMinutes INT',
    ]
    # Create table
    db.add_table('title_basics', columns)
    columns = [
        'tconst VARCHAR(255)',
        'genres VARCHAR(255)',
    ]
    # Create table
    db.add_table('relation_genres', columns)
    with open(filename, 'r') as file:
        # Read column headers 
        columns = clean_split_line(file.readline())
        # Go through each line and insert into table
        print('Loading title_basics')
        for i, line in enumerate(file):
            if i >= num_lines:
                break
            print(f'  Processing line {i+1} / {num_lines}', end='\r')
            title, genre = line_to_data(line, columns)
            db.insert('title_basics', title, commit=(i%commit_frequency == 0))
            for genre in genre['genres'].split(','):
                if genre == '\\N':
                    continue
                db.insert('relation_genres', {'tconst': title['tconst'], 'genres': genre})
        db.commit()
        print('\nDone loading title_basics')
        
def tsv_to_sql_crew(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    def line_to_data(line: str, columns: list[str]) -> dict:
        # print(columns, line)
        split_line = clean_split_line(line)
        director_data = dict([('tconst', split_line[0]), ('nconst', split_line[1])])
        writer_data = dict([('tconst', split_line[0]), ('nconst', split_line[2])])
        return director_data, writer_data
    # Make DDL columns for relation_directors
    columns = [
        'tconst VARCHAR(255)',
        'nconst VARCHAR(255)',
    ]
    # Create table
    db.add_table('relation_directors', columns)
    columns = [
        'tconst VARCHAR(255)',
        'nconst VARCHAR(255)',
    ]
    # Create table
    db.add_table('relation_writers', columns)
    with open(filename, 'r') as file:
        # Read column headers 
        columns = clean_split_line(file.readline())
        # Go through each line and insert into table
        print('Loading title_crew')
        for i, line in enumerate(file):
            if i >= num_lines:
                break
            print(f'  Processing line {i+1} / {num_lines}', end='\r')
            directors, writers = line_to_data(line, columns)
            for director in directors['nconst'].split(','):
                if director == '\\N':
                    continue
                db.insert('relation_directors', {'tconst': directors['tconst'], 'nconst': director})
            for writer in writers['nconst'].split(','):
                if writer == '\\N':
                    continue
                db.insert('relation_writers', {'tconst': writers['tconst'], 'nconst': writer})
        db.commit()
        print('\nDone loading title_crew')

def tsv_to_sql_episode(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    def line_to_data(line: str, columns: list[str]) -> dict:
        # print(columns, line)
        split_line = clean_split_line(line)
        episode_data = dict(zip(columns[:4], split_line[:4]))
        for k in list(episode_data.keys()):
            if episode_data[k] == '\\N':
                del episode_data[k]
                continue
        return episode_data
    # Make DDL columns for relation_episode
    columns = [
        'tconst VARCHAR(255) PRIMARY KEY',
        'parentTconst VARCHAR(255)',
        'seasonNumber INT',
        'episodeNumber INT',
    ]
    # Create table
    db.add_table('relation_episode', columns)
    with open(filename, 'r') as file:
        # Read column headers 
        columns = clean_split_line(file.readline())
        # Go through each line and insert into table
        print('Loading title_episode')
        for i, line in enumerate(file):
            if i >= num_lines:
                break
            print(f'  Processing line {i+1} / {num_lines}', end='\r')
            episodes = line_to_data(line, columns)
            db.insert('relation_episode', episodes)
        db.commit()
        print('\nDone loading title_episode')

def tsv_to_sql_ratings(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    def line_to_data(line: str, columns: list[str]) -> dict:
        # print(columns, line)
        split_line = clean_split_line(line)
        ratings_data = dict(zip(columns[:3], split_line[:3]))
        return ratings_data
    # Make DDL columns for relation_directors
    columns = [
        'tconst VARCHAR(255) PRIMARY KEY',
        'averageRating FLOAT(25)',
        'numVotes INT',
    ]
    # Create table
    db.add_table('relation_ratings', columns)
    with open(filename, 'r') as file:
        # Read column headers 
        columns = clean_split_line(file.readline())
        # Go through each line and insert into table
        print('Loading title_ratings')
        for i, line in enumerate(file):
            if i >= num_lines:
                break
            print(f'  Processing line {i+1} / {num_lines}', end='\r')
            ratings = line_to_data(line, columns)
            db.insert('relation_ratings', ratings)
        db.commit()
        print('\nDone loading title_ratings')

def tsv_to_sql_principals(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    pass

def tsv_to_sql_akas(filename: str, db: MySQLDatabase, num_lines: int = 8000):
    pass


def test_name_basics(num_lines: int = 8000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    sql.drop_table('name_basics')
    sql.drop_table('relation_profession')
    sql.drop_table('relation_titles')
    tsv_to_sql_name(sql, 'data/name.basics.tsv', num_lines)

def test_title_basics(num_lines: int = 8000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    sql.drop_table('title_basics')
    sql.drop_table('relation_genres')
    # tsv_to_sql_title('data/test.title.basics.tsv', sql)
    tsv_to_sql_title('data/title.basics.tsv', sql, num_lines)
    # sql.print_tables()
    # sql.print_data('title_basics')
    # sql.print_data('relation_genres')

def test_title_crew(num_lines: int = 8000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    sql.drop_table('relation_directors')
    sql.drop_table('relation_writers')
    # tsv_to_sql_crew('data/test.title.crew.tsv', sql)
    tsv_to_sql_crew('data/title.crew.tsv', sql, num_lines)
    # sql.print_tables()
    # sql.print_data('relation_directors')
    # sql.print_data('relation_writers')

def test_title_episode(num_lines: int = 8000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    sql.drop_table('relation_episode')
    # tsv_to_sql_episode('data/test.title.episode.tsv', sql)
    tsv_to_sql_episode('data/title.episode.tsv', sql, num_lines)
    # sql.print_tables()
    # sql.print_data('relation_episode')

def test_title_ratings(num_lines: int = 8000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    sql.drop_table('relation_ratings')
    # tsv_to_sql_ratings('data/test.title.ratings.tsv', sql)
    tsv_to_sql_ratings('data/title.ratings.tsv', sql, num_lines)
    # sql.print_tables()
    # sql.print_data('title_ratings')

def filter_sql_data(sql: MySQLDatabase | None = None):
    print('Filtering data')
    if sql is None:
        data = load_secrets()
        sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    print('  Titles       ', end='\r')
    sql.remove('title_basics', "tconst NOT IN (SELECT tconst FROM relation_titles) AND tconst NOT IN (SELECT tconst FROM relation_episode)")
    print('  Genres       ', end='\r')
    sql.remove('relation_genres', "tconst NOT IN (SELECT tconst FROM title_basics)")
    print('  Ratings      ', end='\r')
    sql.remove('relation_ratings', "tconst NOT IN (SELECT tconst FROM title_basics)")
    print('  Episodes     ', end='\r')
    sql.remove('relation_episode', "tconst NOT IN (SELECT tconst FROM title_basics) OR parentTconst NOT IN (SELECT tconst FROM title_basics)")
    print('  Directors    ', end='\r')
    sql.remove('relation_directors', "tconst NOT IN (SELECT tconst FROM title_basics)")
    print('  Writers      ', end='\r')
    sql.remove('relation_writers', "tconst NOT IN (SELECT tconst FROM title_basics)")
    print('  Crew         ', end='\r')
    sql.remove('relation_titles', "tconst NOT IN (SELECT tconst FROM title_basics)")
    print('  People       ', end='\r')
    sql.remove('name_basics', "nconst NOT IN (SELECT nconst FROM relation_directors) AND nconst NOT IN (SELECT nconst FROM relation_writers) AND nconst NOT IN (SELECT nconst FROM relation_titles)")
    print('  Professions  ', end='\r')
    sql.remove('relation_profession', "nconst NOT IN (SELECT nconst FROM name_basics)")
    print('Done filtering data')
    # sql.print_data('title_basics')
    # sql.print_data('name_basics')

def alter_to_keys(sql: MySQLDatabase | None = None):
    if sql is None:
        data = load_secrets()
        sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    print('Altering tables')
    # Check if view exists
    if not sql.check_table_exists('view_crew'):
        query = f"""CREATE VIEW view_crew AS SELECT nconst, tconst FROM relation_titles UNION SELECT nconst, tconst FROM relation_writers UNION SELECT nconst, tconst FROM relation_directors"""
        sql.raw_cmd(query)
    sql.alter('relation_genres', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    sql.alter('relation_ratings', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    # sql.alter('relation_episode', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    sql.alter('relation_directors', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    sql.alter('relation_writers', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    sql.alter('relation_titles', 'ADD FOREIGN KEY (tconst) REFERENCES title_basics(tconst)')
    sql.alter('relation_profession', 'ADD FOREIGN KEY (nconst) REFERENCES name_basics(nconst)')
    # print(sql.get_columns('relation_genres', names_only=False))
    print('Done altering tables')

def load_data(clear_tables: bool = False, filter_data: bool = False, num_lines: int = 100000):
    data = load_secrets()
    # sql = Database()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    if clear_tables:
        current_tables = set([t[0] for t in sql.get_tables()])
        # print('Current tables:', current_tables)
        wanted_tables = set(['name_basics', 'relation_profession', 'relation_titles', 'title_basics', 'relation_genres', 'relation_directors', 'relation_writers', 'relation_episode', 'relation_ratings'])
        # print('Wanted tables:', wanted_tables)
        tables_to_drop = wanted_tables.intersection(current_tables)
        print('Dropping tables:', tables_to_drop)
        sql.drop_table(', '.join(tables_to_drop))
        sql.drop_view('view_crew')
        # sql.print_tables()
    tsv_to_sql_name('data/name.basics.tsv', sql, num_lines)
    tsv_to_sql_title('data/title.basics.tsv', sql, num_lines)
    tsv_to_sql_crew('data/title.crew.tsv', sql, num_lines)
    tsv_to_sql_episode('data/title.episode.tsv', sql, num_lines)
    tsv_to_sql_ratings('data/title.ratings.tsv', sql, num_lines)
    if filter_data:
        filter_sql_data(sql)
        alter_to_keys(sql)
    

def test(skip_load: bool = False):
    if not skip_load:
        num_lines = 100000
        test_name_basics(num_lines)
        test_title_basics(num_lines)
        test_title_crew(num_lines)
        test_title_episode(num_lines)
        test_title_ratings(num_lines)
    filter_sql_data()

def sql_ingest(skip_load: bool = False, filter_data: bool = True, clear_tables: bool = True, num_lines: int = 100000):
    print(skip_load, filter_data, clear_tables, num_lines)
    if not skip_load:
        load_data(clear_tables, filter_data, num_lines)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip_load', action='store_true', help='Skip loading data')
    parser.add_argument('--clear_tables', action='store_false', help='Clear tables')
    parser.add_argument('--filter_data', action='store_false', help='Filter data')
    parser.add_argument('--num_lines', type=int, default=100_000, help='Number of lines to load')
    return parser.parse_args()
    
def main():
    args = parse_args()
    clear_tables = args.clear_tables
    filter_data = args.filter_data
    num_lines = args.num_lines
    skip_load = args.skip_load
    sql_ingest(skip_load, filter_data, clear_tables, num_lines)

if __name__ == '__main__':
    main()