import psycopg2
from tabulate import tabulate

con = psycopg2.connect(
    host="localhost",
    database="production",
    user="postgres",
    password="password"
)

# For isolation: SERIALIZABLE
con.set_isolation_level(3)
# For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    cur.execute("SELECT * FROM Product;")
    rows = cur.fetchall()

    for r in rows:
        print(r)

    con.commit()

except (Exception, psycopg2.DatabaseError) as err:
    print("Error:", err)
    print("Transaction failed — rolling back...")
    con.rollback()

finally:
    if con:
        cur.close()
        con.close()
        print("PostgreSQL connection is now closed")
