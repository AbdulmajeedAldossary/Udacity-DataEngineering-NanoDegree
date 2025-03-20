import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn) -> None:
    """
    Loads data from S3 bucket into Staging tables
    within the RedShift cluster.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn) -> None:
    """
    Process and normalise data from staging tables
    into the Star Schema Design.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    """
    Connects to the RedShift cluster and loads data into
    staging tables as well as inserts data into the
    Star Schema Database Design.
    :return:
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    uri = 'host={} dbname={} user={}' \
          ' password={} port={}'.format(*config['CLUSTER'].values())
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
