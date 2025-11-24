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

    def print_table(name):
        cur.execute(f"SELECT * FROM {name};")
        rows = cur.fetchall()
        print(f"\n{name} table:")
        print(tabulate(rows, headers=[desc[0] for desc in cur.description], tablefmt="psql"))

    def show_state(step):
        print(f"\n=== AFTER STEP {step} ===")
        print_table("Product")
        print_table("Depot")
        print_table("Stock")

    print("=== INITIAL STATE ===")
    print_table("Product")
    print_table("Depot")
    print_table("Stock")

    # 1. Delete product 'p1' from Product and Stock
    cur.execute("DELETE FROM Stock WHERE prod = %s;", ('p1',))
    cur.execute("DELETE FROM Product WHERE prod = %s;", ('p1',))
    show_state(1)

    # 2. Delete depot 'd1' from Depot and Stock
    cur.execute("DELETE FROM Stock WHERE dep = %s;", ('d1',))
    cur.execute("DELETE FROM Depot WHERE dep = %s;", ('d1',))
    show_state(2)

    # 3. Rename product 'p1' to 'pp1' in Product and Stock
    cur.execute("UPDATE Product SET prod = %s WHERE prod = %s;", ('pp1', 'p1'))
    cur.execute("UPDATE Stock SET prod = %s WHERE prod = %s;", ('pp1', 'p1'))
    show_state(3)

    # 4. Rename depot 'd1' to 'dd1' in Depot and Stock
    cur.execute("UPDATE Depot SET dep = %s WHERE dep = %s;", ('dd1', 'd1'))
    cur.execute("UPDATE Stock SET dep = %s WHERE dep = %s;", ('dd1', 'd1'))
    show_state(4)

    # 5. Add product (p100, cd, 5) in Product and (p100, d2, 50) in Stock
    cur.execute("INSERT INTO Product (prod, pname, price) VALUES (%s, %s, %s);", ('p100', 'cd', 5))
    cur.execute("INSERT INTO Stock (prod, dep, quantity) VALUES (%s, %s, %s);", ('p100', 'd2', 50))
    show_state(5)

    # 6. Add depot (d100, Chicago, 100) in Depot and (p1, d100, 100) in Stock
    cur.execute("INSERT INTO Depot (dep, addr, volume) VALUES (%s, %s, %s);", ('d100', 'Chicago', 100))
    cur.execute("INSERT INTO Stock (prod, dep, quantity) VALUES (%s, %s, %s);", ('p1', 'd100', 100))
    show_state(6)

    # Commit all changes
    con.commit()
    print("\nAll operations completed successfully.")

except (Exception, psycopg2.DatabaseError) as err:
    print("Error:", err)
    print("Transaction failed — rolling back...")
    con.rollback()

finally:
    if con:
        cur.close()
        con.close()
        print("\nPostgreSQL connection is now closed")
