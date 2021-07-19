import sqlite3
from sqlite3 import Error
import pandas as pd
import time
import random

# database: biometricupdate.cb
# tables:
#   - raw_articles
#   - enriched_articles

db_file = 'biometricupdate.db'


def read_table(table):
    conn = sqlite3.connect(db_file)

    try:
        df = pd.read_sql_query("SELECT * from " + table, conn)
    except Error as e:
        print(e)

    return df


def write_sitemap_urls(sitemap, df):

    df.set_index('page_index')

    conn = sqlite3.connect(db_file)

    try:
        sql = f"SELECT * FROM sitemap_links WHERE sitemap_url = '{ sitemap }'"
        old = pd.read_sql_query(sql, conn)
        old.set_index('page_index')
        print(f"...{ len(old) } page urls already recorded.")
        df = df[~df['page_url'].isin(old['page_url'])]
    except Error as e:
        print(e)

    print(f"...Recording {len(df) } new page urls.")
    try:
        df.to_sql(name='sitemap_links', con=conn, if_exists='append', index=False)
    except Error as e:
        print(e)


def update_sitemap_status(sitemap, status, table='sitemap_status'):
    conn = sqlite3.connect(db_file)

    sql = f"INSERT OR REPLACE INTO {table}(sitemap_link,sitemap_status) VALUES('{sitemap}','{status}')"

    try:
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
    except Error as e:
        print(e)

def list_tables():

    conn = sqlite3.connect(db_file)

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print(cursor.fetchall())
    except Error as e:
        print(e)


def table_exists(table='raw_articles'):

    conn = sqlite3.connect(db_file)

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{ table }'")
        tables = cursor.fetchall()

        if tables:
            return True
        else:
            return False

    except Error as e:
        return False


def table_status(table='raw_articles'):

    conn = sqlite3.connect(db_file)
    count = ''
    min_date = ''
    max_date = ''

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{ table }'")
        tables = cursor.fetchall()

        if tables:
            cursor.execute(f"SELECT COUNT(*) FROM { table }")
            count = cursor.fetchone()

            cursor.execute(f"SELECT MIN(datePublished) FROM { table }")
            min_date = cursor.fetchone()

            cursor.execute(f"SELECT MAX(datePublished) FROM { table }")
            max_date = cursor.fetchone()

            string = f'The { table } table contains {count[0]} total records ranging from {min_date[0]} to {max_date[0]}.'
            return string
        else:
            return f'{ table } table not found.'

    except Error as e:
        return e


def list_unpulled_articles():
    conn = sqlite3.connect(db_file)

    try:
        df = pd.read_sql_query("SELECT page_url FROM sitemap_links WHERE pulled = 0", conn)
        print(f"{ len(df) } unpulled articles found.")
    except Error as e:
        print(e)

    return df


def mark_as_pulled(link):
    conn = sqlite3.connect(db_file)

    try:
        sql = f"UPDATE sitemap_links SET pulled = 1 WHERE page_url = '{ link }'"
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
    except Error as e:
        print(e)


def write_article_to_db(article_dict):
    conn = sqlite3.connect('biometricupdate.db')

    columns = ', '.join(article_dict.keys())
    placeholders = ', '.join('?' * len(article_dict))
    sql = 'INSERT INTO raw_articles ({}) VALUES ({})'.format(columns, placeholders)
    values = [int(x) if isinstance(x, bool) else x for x in article_dict.values()]

    try:

        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        conn.close()

        time.sleep(random.randint(1,3))
    except Error as e:
        print(e)
        conn.close()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:

        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def initialize_database():
    database = "biometricupdate.db"

    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS raw_articles (
                                        article_index text PRIMARY KEY,
                                        headline TEXT,
                                        author text TEXT,
                                        datePublished TEXT,
                                        dateModified TEXT,
                                        contentCategories TEXT,
                                        articleBody TEXT,
                                        link TEXT
                                    ); """

    sql_create_sitemap_links_table = """ CREATE TABLE IF NOT EXISTS sitemap_links (
                                        page_index TEXT PRIMARY KEY, 
                                        page_url TEXT, 
                                        page_date TEXT,
                                        sitemap_url TEXT,
                                        pulled INTEGER DEFAULT 0
                                    ); """

    sql_create_sitemap_status_table = """ CREATE TABLE IF NOT EXISTS sitemap_status (
                                            sitemap_link TEXT PRIMARY KEY, 
                                            sitemap_status TEXT DEFAULT incomplete
                                        ); """

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:

        create_table(conn, sql_create_projects_table)
        create_table(conn, sql_create_sitemap_links_table)
        create_table(conn, sql_create_sitemap_status_table)

    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    initialize_database()