#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime

def connect_db():
    return psycopg2.connect(
        dbname='kaushalnandaniya',
        user='kaushalnandaniya',
        password='Kaushal@2004',
        host='127.0.0.1',
        port='5432'
    )


def is_connected():
    try:
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.ConnectionError:
        log_connection_issue()
        return False


def log_connection_issue():
    log_file_path = "/Users/kaushalnandaniya/Desktop/Nothing/Python/NetChecker.txt"
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now()}: No internet connection for asset data.\n")


def main():
    if not is_connected():
        print(f"{datetime.now()}: No internet connection. Exiting.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    urls = [
        "https://api.mfapi.in/mf/129046",
        "https://api.mfapi.in/mf/147620",
        "https://api.mfapi.in/mf/127042",
        "https://api.mfapi.in/mf/148454",
        "https://api.mfapi.in/mf/147704",
    ]

    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            fund_name = data.get('meta', {}).get('scheme_name')
            scheme_code = data.get('meta', {}).get('scheme_code')
            latest_nav = data.get('data', [])[0].get('nav') if data.get('data') else None
            nav_date_str = data.get('data', [])[0].get('date') if data.get('data') else None



            if scheme_code and latest_nav and nav_date_str:

               nav_date = datetime.strptime(nav_date_str, "%d-%m-%Y").date()

               check_query = sql.SQL('''
               SET search_path TO ams;
               SELECT 1 FROM ams.assetdata WHERE assetid = %s AND date = %s
               ''')
               cursor.execute(check_query, (scheme_code, nav_date))
               exists = cursor.fetchone()

               if exists:
                  print(f"Skipping insert for {fund_name} with NAV date {nav_date} as it already exists.")
               else:
                  insert_query = sql.SQL('''
                  INSERT INTO ams.assetdata (assetid,date, nav)
                  VALUES (%s, %s, %s)
                  ''')
                  cursor.execute(insert_query, (scheme_code,nav_date, latest_nav))
                  print(f"Inserted: Fund Name: {fund_name}, Latest NAV: {latest_nav}, Date: {nav_date}")

        except requests.exceptions.RequestException as e:
          print(f"Error fetching data from {url}: {e}")
        except psycopg2.Error as e:
          print(f"Database error: {e}")
          conn.rollback()
        except Exception as e:
          print(f"General error: {e}")
          conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
