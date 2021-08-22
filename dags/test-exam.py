import psycopg2
import pandas as pd

from psycopg2 import Error

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta


class Config:
    # POSTGRESQL_HOST = Variable.get("POSTGRESQL_HOST")
    # POSTGRESQL_PORT = Variable.get("POSTGRESQL_PORT")
    # POSTGRESQL_USER = Variable.get("POSTGRESQL_USER")
    # POSTGRESQL_PASSWORD = Variable.get("POSTGRESQL_PASSWORD")
    # POSTGRESQL_DB = Variable.get("POSTGRESQL_DB")

    # POSTGRESQL_TABLE = Variable.get("POSTGRESQL_TABLE")
    # POSTGRESQL_TABLE_COLUMN_NAME = Variable.get("POSTGRESQL_TABLE_COLUMN_NAME")
    # BUCKET_NAME = Variable.get("BUCKET_NAME")
    # FOLDER_NAME = Variable.get("FOLDER_NAME")
    # PROJECT_ID = Variable.get("PROJECT_ID")

    # CREDENTIALS_PATH = Variable.get("CREDENTIALS_PATH")

    POSTGRESQL_HOST = "35.247.174.171"
    POSTGRESQL_PORT = "5432"
    POSTGRESQL_USER = "exam"
    POSTGRESQL_PASSWORD = "bluePiExam"
    POSTGRESQL_DB = "postgres"
    POSTGRESQL_TABLE = "users,user_log"


class Xcoms_class:
    def __init__(self):
        self.ep_df = None
        self.behavior_df = None

    def get_ep(self):
        print(type(self.ep_df))
        return self.ep_df

    def set_ep(self, ep_df):
        self.ep_df = ep_df

    def get_behavior(self):
        print(type(self.behavior_df))
        return self.behavior_df

    def set_behavior(self, behavior_df):
        self.behavior_df = behavior_df


_xcom = Xcoms_class()


def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def postgresql_to_dataframe(conn, select_query, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    tupples = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(tupples, columns=column_names)
    return df


def execute_insert_count_like_to_db(conn, df, table):

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(id,%s) VALUES(DEFAULT,%%s, %%s)" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return False
    print("execute_insert_count_like_to_db() done")
    cursor.close()
    return True


def execute_insert_payment_success_to_db(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(id,%s) VALUES(DEFAULT,%%s, %%s,%%s, %%s,%%s)" % (
        table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_insert_payment_success_to_db() done")
    cursor.close()


def execute_insert_book_to_db(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s)" % (
        table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_insert_book_to_db() done")
    cursor.close()


def execute_insert_episode_to_db(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (
        table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_insert_episode_to_db() done")
    cursor.close()


def get_episode():
    print("get_episode()")
    param_cms = {
        "host": "34.87.117.252",
        "database": "cms",
        "user": "readuser",
        "password": "digital2021testing"
    }
    conn_cms = connect(param_cms)
    column_ep_names = ["id", "book_id", "name"]
    ep_df = postgresql_to_dataframe(
        conn_cms, "select * from episode", column_ep_names)
    # _xcom.set_ep(ep_df)
    # print(_xcom.get_ep())
    # print(_xcom.get_behavior())
    return ep_df


def get_behavior():
    print("get_behavior")
    param_like = {
        "host": "34.87.117.252",
        "database": "likesvc",
        "user": "readuser",
        "password": "digital2021testing"
    }
    conn_like = connect(param_like)
    column_behavior_names = ["behavior", "episode_id", "timestamp"]
    behavior_df = postgresql_to_dataframe(
        conn_like, "select * from behavior", column_behavior_names)
    # _xcom.set_behavior(behavior_df)
    # print(_xcom.get_ep())
    # print(_xcom.get_behavior())
    return behavior_df


def count_like():
    print("count_like")
    ep_df = get_episode()
    behavior_df = get_behavior()
    print("-----------")
    ep_behavior_df = ep_df.join(behavior_df.set_index('episode_id'), on='id')
    count_like_df = ep_behavior_df.groupby(
        ["book_id"]).size().reset_index(name='count')
    print(count_like_df.head())
    return count_like_df


def insert_count_like_to_db():
    print('insert_count_like_to_db')
    df = count_like()
    print(df.columns)
    param_like_count = {
        "host": "6.tcp.ngrok.io",
        "port": "15026",
        "database": "test",
        "user": "postgres",
        "password": "admin"
    }
    conn_test = connect(param_like_count)
    print('execute_insert_count_like_to_db')
    result = execute_insert_count_like_to_db(conn_test, df, 'like_count')
    print(f'result is {result}')


def get_payment():
    param_payment = {
        "host": "34.87.117.252",
        "database": "payment",
        "user": "readuser",
        "password": "digital2021testing"
    }
    # Connect to the database
    conn_payment = connect(param_payment)
    column_payment_names = ["order_id", "status",
                            "timestamp", "episode_id", "amount"]
    payment_df = postgresql_to_dataframe(
        conn_payment, "select * from transaction", column_payment_names)
    return payment_df


def convert_is_payment_success():
    column_payment_names = ["order_id", "status",
                            "timestamp", "episode_id", "amount"]
    payment_df = get_payment()
    sort_payment_df = payment_df.sort_values(by=['order_id'])
    sort_payment_df = sort_payment_df.reset_index(drop=True)
    print('sort_payment_df')
    print(sort_payment_df.head())
    payment_success = pd.DataFrame(columns=column_payment_names)
    for i in sort_payment_df.index:
        indexNow = i
        indexNext = i+1
        if indexNext >= len(sort_payment_df):
            indexNext = i
        if sort_payment_df['status'][indexNow] == 'pending' and sort_payment_df['status'][indexNext] == 'success':
            payment_success = payment_success.append([{
                'order_id': sort_payment_df['order_id'][indexNext],
                'status': sort_payment_df['status'][indexNext],
                'timestamp': sort_payment_df['timestamp'][indexNext],
                'episode_id': sort_payment_df['episode_id'][indexNext],
                'amount': sort_payment_df['amount'][indexNext]
            }], ignore_index=True)
    return payment_success


def insert_payment_success_to_db():
    print('insert_payment_success_to_db')
    df = convert_is_payment_success()
    print(df.columns)
    print(df.head())
    param_test = {
        "host": "6.tcp.ngrok.io",
        "port": "15026",
        "database": "test",
        "user": "postgres",
        "password": "admin"
    }
    conn_test = connect(param_test)
    print('execute_insert_payment_success_to_db')
    result = execute_insert_payment_success_to_db(
        conn_test, df, 'payment_success')
    print(f'result is {result}')


def get_book():
    param_cms = {
        "host": "34.87.117.252",
        "database": "cms",
        "user": "readuser",
        "password": "digital2021testing"
    }
    # Connect to the database
    conn_cms = connect(param_cms)
    column_ep_names = ["id", "name"]
    book_df = postgresql_to_dataframe(
        conn_cms, "select * from book", column_ep_names)
    return book_df


def insert_book_to_db():
    df = get_book()
    print('insert_book_to_db')
    print(df.columns)
    print(df.head())
    param_test = {
        "host": "6.tcp.ngrok.io",
        "port": "15026",
        "database": "test",
        "user": "postgres",
        "password": "admin"
    }
    conn = connect(param_test)
    print('execute_insert_book_to_db')
    result = execute_insert_book_to_db(conn, df, 'book')
    print(f'result is {result}')


def insert_episode_to_db():
    df = get_episode()
    print('insert_episode_to_db')
    print(df.columns)
    print(df.head())
    param_test = {
        "host": "6.tcp.ngrok.io",
        "port": "15026",
        "database": "test",
        "user": "postgres",
        "password": "admin"
    }
    conn = connect(param_test)
    print('execute_insert_episode_to_db')
    result = execute_insert_episode_to_db(conn, df, 'episode')
    print(f'result is {result}')


default_args = {
    'owner': 'VorapratR',
    'start_date': days_ago(1),
    'email': ['voraprat.r@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

dag = DAG(
    'data-eng-exam',
    default_args=default_args,
    description='Interview Challenge (Data Engineer) - 4 days',
    schedule_interval=timedelta(days=1),
)
start_airflow_task = DummyOperator(task_id='start', dag=dag)

# exam1
get_episode_task = PythonOperator(
    task_id='get_episode_task',
    python_callable=get_episode,
    dag=dag
)
get_behavior_task = PythonOperator(
    task_id='get_behavior_task',
    python_callable=get_behavior,
    dag=dag
)
count_like_task = PythonOperator(
    task_id='count_like_task',
    python_callable=count_like,
    dag=dag
)
insert_count_like_to_db_task = PythonOperator(
    task_id='insert_count_like_to_db_task',
    python_callable=insert_count_like_to_db,
    dag=dag
)

# exam2
get_payment_task = PythonOperator(
    task_id='get_payment_task',
    python_callable=get_payment,
    dag=dag
)

convert_is_payment_success_task = PythonOperator(
    task_id='convert_is_payment_success_task',
    python_callable=convert_is_payment_success,
    dag=dag
)

insert_payment_success_to_db_task = PythonOperator(
    task_id='insert_payment_success_to_db_task',
    python_callable=insert_payment_success_to_db,
    dag=dag
)

# exam3
get_book_task = PythonOperator(
    task_id='get_book_task',
    python_callable=get_book,
    dag=dag
)

insert_book_to_db_task = PythonOperator(
    task_id='insert_book_to_db_task',
    python_callable=insert_book_to_db,
    dag=dag
)

# exam4
insert_episode_to_db_task = PythonOperator(
    task_id='insert_episode_to_db_task',
    python_callable=insert_episode_to_db,
    dag=dag
)

end_airflow_task = DummyOperator(task_id='end', dag=dag)

start_airflow_task >> get_episode_task >> get_behavior_task >> count_like_task >> insert_count_like_to_db_task >> end_airflow_task
start_airflow_task >> get_payment_task >> convert_is_payment_success_task >> insert_payment_success_to_db_task >> end_airflow_task
start_airflow_task >> get_book_task >> insert_book_to_db_task >> end_airflow_task
start_airflow_task >> get_episode_task >> insert_episode_to_db_task >> end_airflow_task
