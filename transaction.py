# perform transcation on db
# Problem:
#   The depot d1 is deleted from depot and stock
import psycopg2
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

#%%  perform transaction

with conn.cursor() as cur:
    try:
        cur.execute("""
            DELETE FROM depot
            WHERE depid = 'd1';
        """)
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('cannot delete')
    finally:
        cur.close()
        conn.commit()
        conn.close()
