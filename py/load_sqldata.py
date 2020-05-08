import pyodbc
import numpy as np
import pandas as pd
import sys
from threading import Thread, Lock

def connect_sqlserver(server,dbname,uid=None,pwd=None,port=1433,driver='/usr/local/lib/libmsodbcsql.17.dylib'):
    #- connects to the sql server using pyodbc and returns a cursor object
    if uid is None:
        try: #- windows authentication:
            print("trying windows authentication")
            cnxn=pyodbc.connect(driver=driver,server=server,database=dbname,Trusted_Connection='yes')
        except:
            print("Windows authentication could not be established. Provide uid/pwd for sql driver connection")
            sys.exit(0)
    else:
        print("using sql driver-- non-windows authentication")
        if pwd is None:
            print("Must also provide pwd")
            sys.exit(0)
        else:  
            cnxn=pyodbc.connect(driver=driver,server=server,database=dbname,uid=uid,pwd=pwd,port=port)
    #cursor=cnxn.cursor()
    return cnxn

def load_data(cnxn,sqlquery):
    """
    cursor: pyodbc.cursor object
    sqlquery: sql query string
    returns pandas dataframe from the sqlquery. 
    Use only for small databases if running from stanalone node-- to make efficient 
    need distributed architecture for larger databases 
    """
    cursor=cnxn.cursor()
    data=pd.read_sql(sqlquery,cursor.connection)
    return data

def load_sql_hdf(engine, query, verbose=True, chunksize=1000000):
    """ Return DataFrame from SELECT query and engine

    Given a valid SQL SELECT query and a engine, return a Pandas 
    DataFrame with the response data.

    Args:
        engine: db engine from sqlalchemy
        query: Valid SQL, containing a SELECT query
        verbose: prints chunk progress if True. Default False.
        chunksize: Number of lines to read per chunk. Default 100000

    Returns:
        df: A Pandas DataFrame containing the response of query
    ref: https://www.reddit.com/r/learnpython/comments/9f88cp/i_want_to_load_a_multi_million_row_sql_output/

    """    
    import tempfile
    # get the data to temp chunk filese
    i = 0
    paths_chunks = []
    with tempfile.TemporaryDirectory() as td:
        for df in pd.read_sql_query(sql=query, con=engine, chunksize=chunksize):
            path = td + "/chunk" + str(i) + ".hdf5"
            df.to_hdf(path, key='data')
            if verbose:
                print("wrote", path)
            paths_chunks.append(path)
            i+=1

        # Merge the chunks using concat, the most efficient way AFAIK
        df = pd.DataFrame()
        for path in paths_chunks:
            df_scratch = pd.read_hdf(path)
            df = pd.concat([df, df_scratch])
            if verbose:
                print("read", path)    
    return df


def read_sql_inmem_gzip_pandas_decompress(query, db_engine): #- faster load of data for postgreSQL 
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
       query=query, head="HEADER"
    )
    conn = db_engine.raw_connection()
    cur = conn.cursor()
    store = io.BytesIO()
    with GzipFile(fileobj=store, mode='w') as out:
        cur.copy_expert(copy_sql, out)
    store.seek(0)
    df = pandas.read_csv(store, compression='gzip')
    return df


"""
def write_data_rowwise(cnxn,df):
    cursor=cnxn.cursor()
    for index, row in df.iterrows():
        print(row)
        cursor.execute....
"""

def write_data_table(df,server,dbname,outtable,uid,pwd,port=1433,driver='/usr/local/lib/libmsodbcsql.17.dylib'): 
    from sqlalchemy import create_engine, event
    from urllib.parse import quote_plus
    conn ="DRIVER="+driver+";SERVER="+server+";DATABASE="+dbname+";UID="+uid+";PWD="+pwd
    quoted = quote_plus(conn)
    new_con='mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine=create_engine(new_con)
    #- Execute many for data insert: https://medium.com/analytics-vidhya/speed-up-bulk-inserts-to-sql-db-using-pandas-and-python-61707ae41990
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    print("writing table to the db")
    df.to_sql(outtable,con=engine,if_exists='replace',index=False)
    print("Finish writing table: ", outtable)


class Databwrite_data_table(df,server,dbname,outtable,uid,pwdaseWorker(Thread):
    __lock = Lock()

    def __init__(self, db, query, result_queue):
        Thread.__init__(self)
        self.db = db
        self.query = query
        self.result_queue = result_queue

    def run(self):
        result = None
        logging.info("Connecting to database...")
        try:
            conn = connect(host=host, port=port, database=self.db)
            curs = conn.cursor()
            curs.execute(self.query)
            result = curs
            curs.close()
            conn.close()
        except Exception as e:
            logging.error("Unable to access database %s" % str(e))
        self.result_queue.append(result)

def get_two_parallels(): #- replace the db names and query
    delay = 1
    result_queue = []
    worker1 = DatabaseWorker("db1", "select something from sometable",
        result_queue)
    worker2 = DatabaseWorker("db1", "select something from othertable",
        result_queue)
    worker1.start()
    worker2.start()

    # Wait for the job to be done
    while len(result_queue) < 2:
        sleep(delay)
    job_done = True
    worker1.join()
    worker2.join()


