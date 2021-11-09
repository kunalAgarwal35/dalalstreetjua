import psycopg2

def connect(host,database,user,password,port):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)
    return conn

connect("localhost", "futures_tick_data", "postgres", "postpass@123", "5433")
try:
    connection = psycopg2.connect(user="postgres",
                                  password="postpass@123",
                                  host="localhost",
                                  port="5433",
                                  database="futures_tick_data")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from reliance"

    cursor.execute(postgreSQL_select_Query)
    mobile_records = cursor.fetchall()

    print("Print each row and it's columns values")
    for row in mobile_records:
        print("Id = ", row[0], )
        print("Model = ", row[1])
        print("Price  = ", row[2], "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
def get_vendors():
    """ query data from the vendors table """
    conn = connect("localhost", "futures_tick_data", "postgres", "postpass@123", "5433")
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name")
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()