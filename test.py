import loader
from db import MySQLDatabase

def main():
    # read config file
    data = loader.load_secrets()
    sql = MySQLDatabase(data['host'], data['user'], data['password'], data['database'])
    
    sql.print_databases()
    sql.print_tables()

    sql.add_table('users', 'id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT')
    sql.insert('users', {'name': 'John', 'age': 20})

    sql.print_tables()
    sql.print_data('users')
    # sql.remove('users', 'name = "John"')
    sql.print_data('users')
    # sql.drop_table('users')
    sql.print_tables()

if __name__ == '__main__':
    main()