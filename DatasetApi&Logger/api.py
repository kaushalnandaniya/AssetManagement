import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime
import os

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
        log_file.write(f"{datetime.now()}: No internet connection for asset.\n")


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

            if scheme_code and latest_nav is not None:
                update_query = sql.SQL('''
                SET search_path TO ams;
                UPDATE ams.asset
                SET nav = %s
                WHERE assetid = %s
                ''')
                cursor.execute(update_query, (latest_nav, scheme_code))
                print(f"Updated: Fund Name: {fund_name}, Latest NAV: {latest_nav}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
        except Exception as e:
            print(f"Database error: {e}")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
