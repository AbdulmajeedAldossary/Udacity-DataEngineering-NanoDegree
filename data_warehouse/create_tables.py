import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn) -> None:
    """
    Drops every table specified
    within the sql_queries.py file.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Deleted')


def create_tables(cur, conn) -> None:
    """
    Creates all tables from design
    within the sql_queries.py file.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Created')


def main() -> None:
    """
    Main method. Connects to Amazon AWS RedShift.
    Creates all tables and migrates data.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        uri = 'host={} dbname={} user={}' \
              ' password={} port={}'.format(*config['CLUSTER'].values())
        conn = psycopg2.connect(uri)
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except Exception as exception:
        print(exception)


if __name__ == "__main__":
    main()
