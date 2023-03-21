from flask import Flask, render_template, request, redirect, url_for
import db
import loader
from jinja2 import Environment, FileSystemLoader
import os
import json

app = Flask(__name__)

class url_finder:
    def __init__(self):
        pass
    def get_url(self, url, **kwargs):
        return url_for(url, **kwargs)

finder = url_finder()

def unpack_singletons(singletons):
    return [singleton[0] for singleton in singletons]

# Create a template for the index page
@app.route('/', methods=['GET'])
def index():
    info = loader.load_secrets()
    sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
    # columns = sql.get_columns('users')
    # Get the count of titles and names
    title_count = sql.select(['title_basics', ], columns=['COUNT(*)'])[0][0]
    name_count = sql.select(['name_basics', ], columns=['COUNT(*)'])[0][0]
    # print(f'{title_count = }')
    # print(f'{name_count = }')
    genre_count = sql.select(['relation_genres', ], columns=['COUNT(*)'])[0][0]
    # print(f'{genre_count = }')
    director_count = sql.select(['relation_directors', ], columns=['COUNT(*)'])[0][0]
    # print(f'{director_count = }')
    writer_count = sql.select(['relation_writers', ], columns=['COUNT(*)'])[0][0]
    # print(f'{writer_count = }')
    episode_count = sql.select(['relation_episode', ], columns=['COUNT(*)'])[0][0]
    # print(f'{episode_count = }')
    rating_count = sql.select(['relation_ratings', ], columns=['COUNT(*)'])[0][0]
    # print(f'{rating_count = }')
    cast_count = sql.select(['relation_titles', ], columns=['COUNT(*)'])[0][0]
    # print(f'{cast_count = }')


    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("index.html")
    return render_template(template, finder=finder, title_count=title_count, name_count=name_count)

# Select the columns to load
@app.route('/load/<table>', methods=['GET'])
def load_data(table):
    info = loader.load_secrets()
    sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
    columns = sql.get_columns(table)
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("load_data.html")
    return render_template(template, table=table, columns=columns, table_url=url_for("table"))

# Select the columns to load
@app.route('/name/<nconst>', methods=['GET'])
def view_name(nconst):
    info = loader.load_secrets()
    sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
    # print(sql.get_columns('name_basics'))

    # Get name data 
    column_labels = ['nconst', 'primaryName', 'birthYear', 'deathYear']
    columns = sql.select(['name_basics', ], columns=column_labels, condition=f"nconst='{nconst}'")[0]
    # print(f'{columns = }')
    data = dict(zip(column_labels, columns))

    # print(f'{data = }')
    # print(sql.get_tables())
    # print(sql.get_columns('title_basics'))
    # print(sql.get_columns('relation_titles'))
    # print(sql.get_columns('relation_profession'))

    # Get title data 
    title_labels = ['title_basics.tconst', 'primaryTitle', 'titleType', 'startYear']
    titles = sql.select(['title_basics NATURAL JOIN view_crew'], columns=title_labels, condition=f"nconst='{nconst}'")
    title_labels[0] = 'tconst'
    # print(f'{titles = }')

    # Get profession data
    profession_labels = ['primaryProfession']
    professions = unpack_singletons(sql.select(['relation_profession'], columns=profession_labels, condition=f"nconst='{nconst}'"))
    # print(f'{professions = }')

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("name.html")
    return render_template(template, finder=finder, data=data, title_labels=title_labels, titles=titles, professions=professions)

# Name search
@app.route('/search/name', methods=['POST'])
def search_name():

    pattern = request.form.getlist("name")[0]
    
    info = loader.load_secrets()
    sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])

    # Get name data
    column_labels = ['nconst', 'primaryName', 'birthYear', 'deathYear']
    data = sql.select(['name_basics', ], columns=column_labels, condition=f"primaryName LIKE '%{pattern}%'")
    # print(f'{data = }')

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("search.html")
    return render_template(template, finder=finder, pattern=pattern, columns=column_labels, data=data)

# Title search
@app.route('/search/title', methods=['POST'])
def search_title():
    
        pattern = request.form.getlist("title")[0]
        
        info = loader.load_secrets()
        sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
    
        # Get title data
        column_labels = ['tconst', 'primaryTitle', 'titleType', 'startYear', 'endYear', 'runtimeMinutes', 'averageRating', 'numVotes']
        data = sql.select(['(title_basics NATURAL JOIN relation_ratings)'], columns=column_labels, condition=f"primaryTitle LIKE '%{pattern}%'")
        # print(f'{data = }')
    
        environment = Environment(loader=FileSystemLoader("templates/"))
        template = environment.get_template("search.html")
        return render_template(template, finder=finder, pattern=pattern, columns=column_labels, data=data)

# Select the columns to load
@app.route('/title/<tconst>', methods=['GET'])
def view_title(tconst):
    info = loader.load_secrets()
    sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
    
    # Get title data
    # print(sql.get_columns('title_basics'))
    column_labels = ['tconst', 'primaryTitle', 'titleType', 'startYear', 'endYear', 'runtimeMinutes', 'averageRating', 'numVotes']
    columns = sql.select(['(title_basics NATURAL JOIN relation_ratings)'], columns=column_labels, condition=f"tconst='{tconst}'")
    # Handle missing data
    if columns:
        columns = columns[0]
    else:
        column_labels = ['tconst', 'primaryTitle', 'titleType', 'startYear', 'endYear', 'runtimeMinutes']
        columns = sql.select(['title_basics'], columns=column_labels, condition=f"tconst='{tconst}'")[0]
    # print(f'{columns = }')
    data = dict(zip(column_labels, columns))
    # print(f'{data = }')

    # print(sql.get_tables())
    # print(sql.get_columns('name_basics'))
    # print(sql.get_columns('relation_titles'))
    # print(sql.get_columns('relation_writers'))
    # print(sql.get_columns('relation_genres'))

    # Get episode data
    episode_labels = ['tconst', 'primaryTitle', 'seasonNumber', 'episodeNumber']
    episodes = sql.select(['relation_episode NATURAL JOIN title_basics'], columns=episode_labels, condition=f"parentTconst='{tconst}'")
    # print(f'{episodes = }')
    # sql.print_data('relation_episode')
    if not episodes:
        episode_labels = ['parentTconst', 'primaryTitle']
        episodes = sql.select(['relation_episode JOIN title_basics ON relation_episode.parentTconst=title_basics.tconst'], columns=episode_labels, condition=f"relation_episode.tconst='{tconst}'")
        # print(f'{episodes = }')


    # Get cast data
    name_labels = ['nconst', 'primaryName']
    names = sql.select(['name_basics NATURAL JOIN relation_titles'], columns=name_labels, condition=f"tconst='{tconst}'")
    # print(f'{names = }')

    # Get writer data
    writer_labels = ['nconst', 'primaryName']
    writers = sql.select(['name_basics NATURAL JOIN relation_writers'], columns=writer_labels, condition=f"tconst='{tconst}'")
    # print(f'{writers = }')

    director_labels = ['nconst', 'primaryName']
    directors = sql.select(['name_basics NATURAL JOIN relation_directors'], columns=director_labels, condition=f"tconst='{tconst}'")
    # print(f'{directors = }')

    # Get genre data
    genre_labels = ['genres']
    genres = unpack_singletons(sql.select(['relation_genres'], columns=genre_labels, condition=f"tconst='{tconst}'"))
    # print(f'{genres = }')

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("title.html")
    return render_template(template, finder=finder, data=data, name_labels=name_labels, names=names, writer_labels=writer_labels, writers=writers, director_labels=director_labels, directors=directors, genre_labels=genre_labels, genres=genres, episode_labels=episode_labels, episodes=episodes)

# Create a template to select what type of data to load
@app.route('/load', methods=['GET', 'POST'])
def table_select():
    tables = ('name_basics', 'title_basics')
    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("table_select.html")
    columns = []
    selected_tables=[]
    selected_columns = []
    show_types = []
    # print(f'{request.form = }')
    if request.method == 'POST':
        # print(f'{request.form = }')
        selected_tables = request.form.getlist("selected_tables")
        # print(f'{selected_tables = }')
        selected_columns = request.form.getlist("selected_columns")
        info = loader.load_secrets()
        sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
        # sql.print_tables()
        if selected_columns:
            # print(f'{selected_columns = }')
            # print(f'{selected_tables = }')
            return redirect(url_for("table", finder=finder, tables=selected_tables, columns=selected_columns))
        for table in selected_tables:
            columns.extend(sql.get_columns(table))
            show_types = [v[0] for v in sql.select(['title_basics'], columns=['titleType'], unique=True)]

        # print(columns)
    return render_template(template, finder=finder, tables=tables, columns=columns, selected_tables=selected_tables, show_types=show_types, table_url=url_for("table", tables=selected_tables, columns=selected_columns), table_select_url=url_for("table_select"))

# Create a template for the login page
@app.route('/table', methods=['POST'])
def table():
    def get_table_data(tables: list[str], columns: list[str], last=None) -> list[list[str]]:
        info = loader.load_secrets()
        sql = db.MySQLDatabase(info['host'], info['user'], info['password'], info['database'])
        # print(f'{crew = }')
        if 'name_basics' in tables and 'title_basics' in tables:
            # Explicitly update nconst and tconst to use name_basics.nconst and title_basics.tconst
            # print(columns)
            # Create subquery to join the tables
            tables = [f"""(name_basics NATURAL JOIN view_crew NATURAL JOIN title_basics)"""]
            # print(tables)
        condition = "" if not last else f"{last[0]}const > '{last}'"
        if request.form.getlist("show_type"):
            show_types = request.form.getlist("show_type")
            if condition:
                condition += " AND "
            condition += f"titleType IN {tuple(show_types)}" if len(show_types) > 1 else f"titleType = '{show_types[0]}'"
        data = sql.select(tables, columns=columns, condition=condition, limit=1000)
        return data
    columns = request.form.getlist("selected_columns")
    tables = request.form.getlist("selected_tables")
    last = request.form.get("last")
    # print(f'{columns = }')
    # print(f'{tables = }')
    # print(f'{last = }')
    if not columns:
        return redirect(url_for("index"))
    if not tables:
        return redirect(url_for("index"))
    data = get_table_data(tables, columns, last)

    # print(f'{data = }')

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("table.html")
    return render_template(template, finder=finder, columns=columns, tables=tables, data=data, index_url=url_for("index"))
