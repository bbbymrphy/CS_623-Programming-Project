# CS 623 Programming Project

# Robert Murphy, 7/6/2024


import os
import psycopg2 as psql

conn = psql.connect(dbname = "postgres",
                        user = "bobby",
                        host= 'localhost',
                        password = "1234",
                        port = '5432')
print(conn)

#For isolation: SERIALIZABLE
conn.set_isolation_level(3)

#For atomicity
conn.autocommit = False

#%%  CREATE TABLES
with conn.cursor() as cur:

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product(
        pid CHAR(2),
        pname CHAR(20),
        price FLOAT);
         """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS depot(
        depid  CHAR(2),
        addr CHAR(10), 
        volume INTEGER);
        """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock(
        prodid CHAR(2),
        depid CHAR(2),
        quantity INTEGER);
        """)
    conn.commit()
cur.close

conn.close


#%% CREATE CONSTRIANTS
with conn.cursor() as cur:
    try:
        cur.execute("""
            ALTER TABLE product ADD CONSTRAINT pk_product PRIMARY KEY (pid);
            """)
        conn.commit()
        print('constraint added in product')
        cur.close
        conn.close
    except (Exception, psql.DatabaseError) as err:
        print(err)
        conn.rollback()
        cur.close
        conn.close

    try:
        cur.execute("""
            ALTER TABLE depot ADD CONSTRAINT pk_depot PRIMARY KEY (depid);
            """)
        conn.commit()
        print('constraint added in depot')
        cur.close
        conn.close
    except (Exception, psql.DatabaseError) as err:
        print(err)
        conn.rollback()
        cur.close
        conn.close
    try:
        cur.execute("""
            ALTER TABLE stock ADD CONSTRAINT fk_stock_pid FOREIGN KEY (prodid) REFERENCES product(pid) ON DELETE CASCADE;
            ALTER TABLE stock ADD CONSTRAINT fk_stock_did FOREIGN KEY (depid)  REFERENCES depot(depid) ON DELETE CASCADE;
            """)
        conn.commit()
        print('constraint added in stock')
        cur.close
        conn.close
    except (Exception, psql.DatabaseError) as err:
        print(err)
        print("PostgreSQL connection is now closed, Contraints Already Exist")
        conn.rollback()
        cur.close
        conn.close



#%% INSERT DATA
# data will only insert if it meets table constraints -- consistency req

with conn.cursor() as cur:
        path = r'/Users/bobbymurphy/Documents/school/pace/cs623/programming_projec/'
        csvs = ['product.csv', 'depot.csv', 'stock.csv']
        tables = ['product', 'depot', 'stock']

        for csv,table in zip(csvs,tables):
            try:
                with open(path+csv, 'r') as file:

                    cur.copy_from(file,table,sep=',')
            except (Exception, psql.DatabaseError) as err:

                print(err)
                conn.rollback()
                cur.close
                conn.close

cur.close()
conn.commit()
conn.close()

#%% Remove









