import psycopg2
import requests
import json
import time
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

#this logs the messages returned from GEOAPIFY, non-critical code
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GeoapifyAttractionsScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.geoapify.com/v2/places"
        self.categories = ["entertainment", "tourism", "natural"]
        self.all_attractions = []

    def calculate_bounding_box(self, lat, lon, radius_km=50):
        # Sets a with coordinates around the cities you search
        lat_diff = radius_km / 111
        lon_diff = radius_km / (111 * abs(math.cos(math.radians(lat))))

        return {
            'min_lon': lon - lon_diff,
            'min_lat': lat - lat_diff,
            'max_lon': lon + lon_diff,
            'max_lat': lat + lat_diff
        }

    def fetch_attractions_for_location(self, lat, lon, category):
        bbox = self.calculate_bounding_box(lat, lon)
        #The request sent to geoapify
        params = {
            "apiKey": self.api_key,
            "categories": category,
            "filter": f"rect:{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']},{bbox['min_lat']}",
            "limit": 100
        }

        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers={'Accept': 'application/json'}
            )

            response.raise_for_status()
            data = response.json()

            attractions = []
            for place in data.get('features', []):
                #description
                properties = place.get('properties', {})
                geometry = place.get('geometry', {})

                coordinates = geometry.get('coordinates', [None, None])
                lon_coord = coordinates[0] if len(coordinates) > 0 else None
                lat_coord = coordinates[1] if len(coordinates) > 1 else None

                attractions.append({
                    'name': properties.get('name', 'N/A'),
                    'address': properties.get('address_line2', 'N/A'),
                    'zipcode': properties.get('postcode', 'N/A'),
                    'location': {
                        'latitude': lat_coord,
                        'longitude': lon_coord
                    },
                    'description': properties.get('description', 'N/A'),
                    'category': category
                })

            logger.info(f"Fetched {len(attractions)} {category} attractions at ({lat}, {lon})")
            return attractions

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {category} at ({lat}, {lon}): {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error for {category} at ({lat}, {lon}): {e}")
            return []

    def scrape_us_attractions(self):
        # 60 major US cities with at least one per each state
        us_cities = [
            {"name": "New York", "lat": 40.7128, "lon": -74.0060},
            {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
            {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
            {"name": "Houston", "lat": 29.7604, "lon": -95.3698},
            {"name": "Phoenix", "lat": 33.4484, "lon": -112.0740},
            {"name": "Philadelphia", "lat": 39.9526, "lon": -75.1652},
            {"name": "San Antonio", "lat": 29.4241, "lon": -98.4936},
            {"name": "San Diego", "lat": 32.7157, "lon": -117.1611},
            {"name": "Dallas", "lat": 32.7767, "lon": -96.7970},
            {"name": "San Jose", "lat": 37.3382, "lon": -121.8863},
            {"name": "Anchorage, AK", "lat": 61.2181, "lon": -149.9003},
            {"name": "Birmingham, AL", "lat": 33.5207, "lon": -86.8025},
            {"name": "Little Rock, AR", "lat": 34.7465, "lon": -92.2896},
            {"name": "Phoenix, AZ", "lat": 33.4484, "lon": -112.0740},
            {"name": "Sacramento, CA", "lat": 38.5816, "lon": -121.4944},
            {"name": "Denver, CO", "lat": 39.7392, "lon": -104.9903},
            {"name": "Hartford, CT", "lat": 41.7658, "lon": -72.6734},
            {"name": "Dover, DE", "lat": 39.1582, "lon": -75.5244},
            {"name": "Tallahassee, FL", "lat": 30.4383, "lon": -84.2807},
            {"name": "Atlanta, GA", "lat": 33.7490, "lon": -84.3880},
            {"name": "Honolulu, HI", "lat": 21.3069, "lon": -157.8583},
            {"name": "Boise, ID", "lat": 43.6150, "lon": -116.2023},
            {"name": "Springfield, IL", "lat": 39.7817, "lon": -89.6501},
            {"name": "Indianapolis, IN", "lat": 39.7684, "lon": -86.1581},
            {"name": "Des Moines, IA", "lat": 41.6005, "lon": -93.6091},
            {"name": "Topeka, KS", "lat": 39.0558, "lon": -95.6890},
            {"name": "Frankfort, KY", "lat": 38.2009, "lon": -84.8733},
            {"name": "Baton Rouge, LA", "lat": 30.4515, "lon": -91.1871},
            {"name": "Augusta, ME", "lat": 44.3106, "lon": -69.7795},
            {"name": "Annapolis, MD", "lat": 38.9784, "lon": -76.4922},
            {"name": "Boston, MA", "lat": 42.3601, "lon": -71.0589},
            {"name": "Lansing, MI", "lat": 42.7325, "lon": -84.5555},
            {"name": "St. Paul, MN", "lat": 44.9537, "lon": -93.0900},
            {"name": "Jackson, MS", "lat": 32.2988, "lon": -90.1848},
            {"name": "Jefferson City, MO", "lat": 38.5767, "lon": -92.1735},
            {"name": "Helena, MT", "lat": 46.5891, "lon": -112.0391},
            {"name": "Lincoln, NE", "lat": 40.8136, "lon": -96.7026},
            {"name": "Carson City, NV", "lat": 39.1638, "lon": -119.7674},
            {"name": "Concord, NH", "lat": 43.2081, "lon": -71.5376},
            {"name": "Trenton, NJ", "lat": 40.2206, "lon": -74.7597},
            {"name": "Santa Fe, NM", "lat": 35.6870, "lon": -105.9378},
            {"name": "Albany, NY", "lat": 42.6526, "lon": -73.7562},
            {"name": "Raleigh, NC", "lat": 35.7796, "lon": -78.6382},
            {"name": "Bismarck, ND", "lat": 46.8083, "lon": -100.7837},
            {"name": "Columbus, OH", "lat": 39.9612, "lon": -82.9988},
            {"name": "Oklahoma City, OK", "lat": 35.4676, "lon": -97.5164},
            {"name": "Salem, OR", "lat": 44.9429, "lon": -123.0351},
            {"name": "Harrisburg, PA", "lat": 40.2732, "lon": -76.8867},
            {"name": "Providence, RI", "lat": 41.8240, "lon": -71.4128},
            {"name": "Columbia, SC", "lat": 34.0007, "lon": -81.0348},
            {"name": "Pierre, SD", "lat": 44.3683, "lon": -100.3510},
            {"name": "Nashville, TN", "lat": 36.1627, "lon": -86.7816},
            {"name": "Austin, TX", "lat": 30.2672, "lon": -97.7431},
            {"name": "Salt Lake City, UT", "lat": 40.7608, "lon": -111.8910},
            {"name": "Montpelier, VT", "lat": 44.2601, "lon": -72.5754},
            {"name": "Richmond, VA", "lat": 37.5407, "lon": -77.4360},
            {"name": "Olympia, WA", "lat": 47.0379, "lon": -122.9007},
            {"name": "Charleston, WV", "lat": 38.3498, "lon": -81.6326},
            {"name": "Madison, WI", "lat": 43.0731, "lon": -89.4012},
            {"name": "Cheyenne, WY", "lat": 41.1400, "lon": -104.8202}
        ]

        with ThreadPoolExecutor(max_workers=3) as executor:
            #this function serves to stagger requests so that the API is limited according to the free plan
            futures = []
            for city in us_cities:
                for category in self.categories:
                    future = executor.submit(
                        self.fetch_attractions_for_location,
                        city['lat'], city['lon'], category
                    )
                    futures.append(future)
                    #force delay to not spam the api
                    time.sleep(0.5)

            for future in as_completed(futures):
                self.all_attractions.extend(future.result())

    def save_to_json(self, filename='us_attractions.json'):
        # Remove duplicates
        unique_attractions = {
            json.dumps(attraction, sort_keys=True) for attraction in self.all_attractions
        }
        attractions_list = [json.loads(attraction) for attraction in unique_attractions]
        #dumps to us_attractions.json
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(attractions_list, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(attractions_list)} unique attractions to {filename}")



def sumbitToDB():
    # Connect to the local PostgreSQL database
    # to get this working, create a user admin with password adminpass and give it all priviledges, then run \conninfo to get the rest
    conn = psycopg2.connect(database="benashkenazi",
                            host="localhost",
                            user="admin",
                            password="adminpass",
                            port="5432")
    cur = conn.cursor()

    #change this to true if you want to create the database
    insertAllValues = False
    if(insertAllValues):
        #this code wipes the database, wipe only if needed
        #cur.execute("""DROP TABLE IF EXISTS attraction_list; DROP TABLE IF EXISTS attractions;DROP TABLE IF EXISTS users;""")

        # Create tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS attractions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                address TEXT NOT NULL,
                category VARCHAR(50),
                description TEXT,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                zipcode VARCHAR(20)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_name VARCHAR(50) PRIMARY KEY, 
                password VARCHAR(50) NOT NULL,
                last_name VARCHAR(75) NOT NULL,
                first_name VARCHAR(50) NOT NULL,
                favorite_zipcode VARCHAR(50),
                phone_number VARCHAR(15) NOT NULL,
                email VARCHAR(50) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS attraction_list (
                user_name VARCHAR(50) REFERENCES users(user_name) ON DELETE CASCADE,
                attraction_id INT REFERENCES attractions(id) ON DELETE CASCADE,
                PRIMARY KEY (user_name, attraction_id)
            );
        """)

        # Insert initial user
        cur.execute("""
            INSERT INTO users (user_name, password, last_name, first_name, favorite_zipcode, phone_number, email)
            VALUES ('benash', 'tester', 'ashkenazi', 'ben', '85281', '14084443259', 'bashkena@asu.edu')
            ON CONFLICT (user_name) DO NOTHING;
        """)

        # Load attractions data from JSON
        with open('us_attractions.json', 'r') as file:
            attractions = json.load(file)
            for attraction in attractions:
                location = attraction['location']
                cur.execute("""
                    INSERT INTO attractions (name, address, category, description, latitude, longitude, zipcode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    attraction['name'],
                    attraction['address'],
                    attraction['category'],
                    attraction['description'],
                    location['latitude'],
                    location['longitude'],
                    attraction['zipcode']
                ))

        # Insert favorite attractions for user
        for attraction_id in [1, 2, 3]:
            cur.execute("""
                INSERT INTO attraction_list (user_name, attraction_id)
                VALUES ('benash', %s)
                ON CONFLICT (user_name, attraction_id) DO NOTHING;
            """, (attraction_id,))

        # Commit changes
        conn.commit()

    # Displays sample values from each table
    for table in ['attractions', 'users', 'attraction_list']:
        cur.execute(f"SELECT * FROM {table};")
        rows = cur.fetchall()
        print(f"Sample from {table}:")
        for row in rows[:5]:
            print(row)
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"{table} has {count} rows.\n")

    # Close connection
    cur.close()
    conn.close()


def main():
    #checks whether or not you already have the JSON file
    if(not os.path.exists("us_attractions.json")):
        #You can use the data I already pulled, but if you for some reason need new data, go here to create a new api key: https://myprojects.geoapify.com/projects
        API_KEY = ""
        scraper = GeoapifyAttractionsScraper(API_KEY)
        scraper.scrape_us_attractions()
        scraper.save_to_json()
    #inits DB and adds each attraction to it
    sumbitToDB()


main()