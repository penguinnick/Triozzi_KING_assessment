import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask, request, render_template, redirect, url_for
from datetime import datetime, timezone
from amadeus import Client, ResponseError, Location
import pandas as pd
import json
from flask_sqlalchemy import SQLAlchemy

# CREATE TABLES
# create flight searches table to store individual searches with PK field and timestamp
CREATE_SEARCHES_TABLE = (
    "CREATE TABLE IF NOT EXISTS searches (id SERIAL PRIMARY KEY, origin TEXT, destination TEXT, departuredate TIMESTAMP, search_date TIMESTAMP);"
)

CREATE_RESULTS_TABLE = ("""CREATE TABLE IF NOT EXISTS result_tbl (
    id SERIAL PRIMARY KEY,
    search_id INTEGER, 
    search_results JSON,
    FOREIGN KEY(search_id) REFERENCES searches(id) ON DELETE CASCADE);"""
)


TEST_CREATE_RESULTS_TABLE = ("""CREATE TABLE IF NOT EXISTS amadeus_tbl (
    id SERIAL PRIMARY KEY,
    search_id INTEGER, 
    search_results JSON,
    FOREIGN KEY(search_id) REFERENCES searches(id) ON DELETE CASCADE);"""
)

INSERT_RESULT_AMADEUS = "INSERT INTO result_tbl (search_id, search_results)  VALUES (%s, %s)" 
# ADD RECORDS
# insert search record and timestamp into searches table, return PK
INSERT_SEARCH_RETURN_ID = "INSERT INTO searches (origin, destination, departuredate, search_date) VALUES (%s, %s, %s, %s) RETURNING id"
# insert search results into results table
INSERT_RESULT = "INSERT INTO result_tbl (search_id, search_results)  VALUES (%s, %s)" 

# QUERIES
# query all searches
QRY_SEARCHES_ALL = ("SELECT row_to_json(t) FROM (select * from searches) t")
# query searches by id
QUERY_RESULTS_BY_SEARCH_ID = ("SELECT search_results FROM result_tbl WHERE CAST ( search_results ->> 'search_id' AS INTEGER) = %s")

# CLEAR SEARCHES
# clears results and searches (drops results table, cascade removes searches)
CLEAR_SEARCH_RESULTS = ("DROP TABLE result_tbl")
CLEAR_SEARCHES = ("DROP TABLE searches")

# app settings
load_dotenv()

app = Flask(__name__)

# establish connection to database
url = os.getenv("DATABASE_URL")
# set connection via psycopg2
connection = psycopg2.connect(url)

# amadeus API key
amadeus = Client(
    client_id='713U0VxTSiriqvw5gyFtXB8M9SJrOLWq',
    client_secret='IPgOJPRKwkqsISA5'
)

# Home Page
@app.route("/")
def home():
    return render_template("index.html")

# retrieve (all) search criteria
@app.route("/savedflights")
def show_saved():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(QRY_SEARCHES_ALL)
            qry = cursor.fetchall()
            allsearches = pd.read_json(json.dumps(qry), orient='records')   
    return render_template("saved-flights.html", allsearches=allsearches[0])

# call up results by clicking existing search record
@app.route("/savedflights/<search_id>")
def retrieve_saved(search_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(QUERY_RESULTS_BY_SEARCH_ID, (search_id,))
            saved_search = cursor.fetchall()
            res_df = pd.read_json(json.dumps(saved_search), orient='records')
    return render_template('saved-results.html', tables=res_df[0])

# execute search, redirect to result page
@app.route("/search/flights", methods=['POST', 'GET'])
def search_flights():
    origin = request.form['from']
    destination = request.form['destination']
    departuredate = request.form['date']
    search_date = datetime.now(timezone.utc)
    cur = request.form['currency_choice']
    if request.method == "POST":
        try:    
            origin = request.form['from']
            destination = request.form['destination']
            departuredate = request.form['date']
            passengers = request.form['passengers']
            response = amadeus.shopping.flight_offers_search.get(
            originLocationCode=origin,
            destinationLocationCode=destination,
            departureDate=departuredate,
            adults=passengers)
            rd = response.data
            nresults = len(rd)
        except:
            errors.append(
                "Unable to get URL. Please make sure it's valid and try again."
            )
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_SEARCHES_TABLE)
                cursor.execute(INSERT_SEARCH_RETURN_ID, (origin, destination, departuredate, search_date))
                searchid = cursor.fetchone()[0]
                results = {tuple([searchid])}
                with open('results.json', 'w') as result_json:
                    json.dump(list(results), result_json)
                def write_json(result, filename='results.json'): 
                        with open('results.json', "r+") as file:
                            data = json.load(file)
                            data[0].append(result)
                            file.seek(0)
                            json.dump(data, file)
                def count_layovers(response_data):
                    n = len(response_data['itineraries'][0]['segments'])
                    if n==1:
                        layovers = 0
                    else:
                        layovers = n-1
                    return layovers
                def convert_currency(response_data, cur=cur):
                    r = response_data
                    p = float(r['price']['total'])
                    if cur=="USD":                        
                        return round(p*1.06,2)
                    if cur=="HRK":
                        return round(p*7.535141,2)
                    return p
                for i in rd:
                    layovers = count_layovers(i)
                    price = convert_currency(i)
                    results = {'search_id': searchid, 'result_id': int(i['id']), "originCode": origin, "destinationCode": destination, "layovers": layovers, "departuredate": departuredate, "price": price,
                    "currency": cur, "passengers": passengers
                    }
                    write_json(results)
                out = pd.read_json('results.json', typ='series')
    return redirect(url_for("show_results"))

# loads locally stored results and provides them in a table
@app.route("/search/results", methods=['GET'])
def show_results():
    out = pd.read_json('results.json', typ='series')
    searchid = out[0][0]
    res_df = pd.json_normalize(out[0][1:])
    with open('results.json', 'r') as file:
        data = json.load(file)
        results = data[0][1:]
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_RESULTS_TABLE)
            for i in results:
                cursor.execute(INSERT_RESULT, (searchid, json.dumps(i)))
    return render_template('search-results.html', tables2=out[0])

# route for clearing searches.
@app.route("/clear")
def clear_results():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CLEAR_SEARCH_RESULTS)
            cursor.execute(CLEAR_SEARCHES)
    return redirect(url_for("home"))

