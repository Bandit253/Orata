
import datetime
import os
import psycopg2
from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine

class postgres():

    def __init__(self, configfile:str):
        self.params = self.readconfig(configfile)
        self.user = self.params['user']
        self.host = self.params['host']
        self.port = self.params['port']
        self.database = self.params['database']
        self.password = self.params['password']

    def readconfig(self, fname:str, section='postgresql'):
        parser = ConfigParser()
        parser.read(fname)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(f'Section {section} not found in the {fname} file')
        return db

    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            params = self.params
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            print(db_version)
        
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
        return 

    def update(self, sql):
        conn = None
        updated_rows = 0
        try:
            params = self.params
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(sql)
            updated_rows = cur.rowcount
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return updated_rows


    def querydb(self, sql):
        conn = None
        try:
            params = self.params
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            result = []
            cur.execute(sql)
            response = cur.fetchall()
            for row in response:
                result.append(row)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return result

    def export(self, table: str, outfile:str) -> str:
        appPath = os.path.dirname(os.path.realpath(__file__))
        logpath = os.path.join(appPath, 'export_data')
        today = datetime.datetime.now().strftime('%d%m%y_%H:%M:%S')
        ofile = os.path.join(logpath, f"{outfile}_{today}.csv'")
        if not os.path.exists(logpath):
            os.mkdir(logpath)
        try:
            params = self.params
            conn = psycopg2.connect(**params)
            db_df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            db_df.to_csv(ofile, index=False)
            conn.close()
        except Exception as e:
            print(e)   
        return ofile

    def insert(self, table:str, data: pd.DataFrame):
        try:
            engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
            with engine.connect() as connection:
                data.to_sql(name=table, con=connection, index=False, if_exists='append')
        except Exception as e:
            print(e)
        return
    
    def dffromsql(self, sql):
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
        df = pd.read_sql_query(sql ,con=engine)
        return df



def main():
    db = postgres(r'D:\_MlTrader\config\database.ini')
    sql = """select id, product_id, filled_size, side, model """
    sql += """ from trades """
    sql += f""" where extract(EPOCH from now()::timestamp - done_at::timestamp) > delay """
    sql += """ and sold = '0' ; """
    res = db.querydb(sql)
    print(res)

if __name__ == '__main__':
    main()
