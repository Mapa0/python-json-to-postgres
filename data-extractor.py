import requests
import psycopg2
import json

def getData():
    reqUrl = "https://jsonplaceholder.typicode.com/users"
    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.io)" 
    }
    payload = ""
    response = requests.request("GET", reqUrl, data=payload,  headers=headersList)
    return response.text

def connectSqlServer():
    try:
        conn = psycopg2.connect("dbname='mapadb' user='postgres' host='localhost' password='postgres'")
    except:
        print ("Não foi possível se con")
    return conn

def query():
    cur = connectSqlServer().cursor()
    cur.execute("""SELECT datname from pg_database""")
    rows = cur.fetchall()
    return rows

def insertJson():
    tablename = ""
    conn = connectSqlServer()
    cur = conn.cursor()
    data = getData()
    dataDict = json.loads(data)
    for a in dataDict:
        id = (a['id'])
        name = (a['name'])
        username = (a['username'])
        email = (a['email'])
        street = (a['address']['street'])
        suite = (a['address']['suite'])
        city = (a['address']['city'])
        zipcode = (a['address']['zipcode'])
        lat = (a['address']['geo']['lat'])
        long = (a['address']['geo']['lng'])
        phone = (a['phone'])
        website = (a['email'])
        company_name = (a['company']['name'])
        company_catchphrase = (a['company']['catchPhrase'])
        company_bs = (a['company']['bs'])
        sql = f"insert into bronze.default (id, name, username, email, street, suite, city, zipcode, lat, long, phone, website, company_name, company_catchphrase, company_bs) VALUES ({id}, '{name}', '{username}', '{email}', '{street}', '{suite}', '{city}', '{zipcode}', {lat}, {long}, '{phone}', '{website}', '{company_name}', '{company_catchphrase}', '{company_bs}')"
        cur.execute(sql)
    cur.execute("SELECT * FROM bronze.default")
    for i in cur.fetchall():
        print(i)
    conn.commit()
    conn.close()

insertJson()
