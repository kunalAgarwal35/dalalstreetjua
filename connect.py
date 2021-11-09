import psycopg2

def connect(host,database,user,password,port):
	conn = psycopg2.connect(
	    host=host,
	    database=database,
	    user=user,
	    password=password,
	    port=port)
	return conn