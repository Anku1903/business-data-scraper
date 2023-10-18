
import streamlit as st
import pandas as pd
from PIL import Image
import pathlib
from urllib.parse import urlencode
import asyncio
from pyppeteer import launch
from pyppeteer_stealth import stealth
import random
import psycopg2


DB_HOST = "localhost"
DB_NAME = "yellowpage"
DB_USER = "postgres"
DB_PASSWORD = "ps190320"
TABLE_NAME = "output"


result_df = pd.DataFrame(columns=["name","phone","website","link"])
us_states_abbr = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

us_states_full_names = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

us_state_abbreviations = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}



us_states_cities = {
    'Alabama': ['Birmingham', 'Montgomery', 'Huntsville', 'Mobile', 'Tuscaloosa', 'Hoover', 'Dothan', 'Auburn', 'Decatur', 'Madison'],
    'Alaska': ['Anchorage', 'Fairbanks', 'Juneau', 'Sitka', 'Ketchikan', 'Wasilla', 'Kenai', 'Kodiak', 'Bethel', 'Palmer'],
    'Arizona': ['Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Glendale', 'Scottsdale', 'Gilbert', 'Tempe', 'Peoria', 'Surprise'],
    'Arkansas': ['Little Rock', 'Fort Smith', 'Fayetteville', 'Springdale', 'Jonesboro', 'North Little Rock', 'Conway', 'Rogers', 'Pine Bluff', 'Bentonville'],
    'California': ['Los Angeles', 'San Diego', 'San Jose', 'San Francisco', 'Fresno', 'Sacramento', 'Long Beach', 'Oakland', 'Bakersfield', 'Anaheim'],
    'Colorado': ['Denver', 'Colorado Springs', 'Aurora', 'Fort Collins', 'Lakewood', 'Thornton', 'Arvada', 'Westminster', 'Pueblo', 'Centennial'],
    'Connecticut': ['Bridgeport', 'New Haven', 'Stamford', 'Hartford', 'Waterbury', 'Norwalk', 'Danbury', 'New Britain', 'West Hartford', 'Bristol'],
    'Delaware': ['Wilmington', 'Dover', 'Newark', 'Middletown', 'Smyrna', 'Milford', 'Seaford', 'Georgetown', 'Elsmere', 'New Castle'],
    'Florida': ['Jacksonville', 'Miami', 'Tampa', 'Orlando', 'St. Petersburg', 'Hialeah', 'Tallahassee', 'Fort Lauderdale', 'Port St. Lucie', 'Cape Coral'],
    'Georgia': ['Atlanta', 'Columbus', 'Augusta', 'Macon', 'Savannah', 'Athens', 'Sandy Springs', 'Roswell', 'Johns Creek', 'Albany'],
    'Hawaii': ['Honolulu', 'East Honolulu', 'Pearl City', 'Hilo', 'Kailua', 'Waipahu', 'Kaneohe', 'Mililani Town', 'Kahului', 'Ewa Gentry'],
    'Idaho': ['Boise', 'Meridian', 'Nampa', 'Idaho Falls', 'Pocatello', 'Caldwell', 'Twin Falls', 'Lewiston', 'Post Falls'],
    'Illinois': ['Chicago', 'Aurora', 'Rockford', 'Joliet', 'Naperville', 'Springfield', 'Peoria', 'Elgin', 'Waukegan', 'Champaign'],
    'Indiana': ['Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend', 'Carmel', 'Fishers', 'Bloomington', 'Hammond', 'Gary', 'Lafayette'],
    'Iowa': ['Des Moines', 'Cedar Rapids', 'Davenport', 'Sioux City', 'Iowa City', 'Waterloo', 'Ames', 'West Des Moines', 'Council Bluffs', 'Dubuque'],
    'Kansas': ['Wichita', 'Overland Park', 'Kansas City', 'Topeka', 'Olathe', 'Lawrence', 'Shawnee', 'Manhattan', 'Lenexa', 'Salina'],
    'Kentucky': ['Louisville', 'Lexington', 'Bowling Green', 'Owensboro', 'Covington', 'Hopkinsville', 'Richmond', 'Florence', 'Georgetown', 'Elizabethtown'],
    'Louisiana': ['New Orleans', 'Baton Rouge', 'Shreveport', 'Lafayette', 'Lake Charles', 'Kenner', 'Bossier City', 'Monroe', 'Alexandria', 'Houma'],
    'Maine': ['Portland', 'Lewiston', 'Bangor', 'South Portland', 'Auburn', 'Biddeford', 'Sanford', 'Augusta', 'Saco', 'Westbrook'],
    'Maryland': ['Baltimore', 'Columbia', 'Germantown', 'Silver Spring', 'Waldorf', 'Frederick', 'Ellicott City', 'Rockville', 'Gaithersburg', 'Dundalk'],
    'Massachusetts': ['Boston', 'Worcester', 'Springfield', 'Lowell', 'Cambridge', 'New Bedford', 'Brockton', 'Quincy', 'Lynn', 'Fall River'],
    'Michigan': ['Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Lansing', 'Ann Arbor', 'Flint', 'Dearborn', 'Livonia', 'Westland'],
    'Minnesota': ['Minneapolis', 'St. Paul', 'Rochester', 'Duluth', 'Bloomington', 'Brooklyn Park', 'Plymouth', 'St. Cloud', 'Eagan', 'Woodbury'],
    'Mississippi': ['Jackson', 'Gulfport', 'Southaven', 'Hattiesburg', 'Biloxi', 'Meridian', 'Tupelo', 'Olive Branch', 'Greenville', 'Horn Lake'],
    'Missouri': ['Kansas City', 'St. Louis', 'Springfield', 'Columbia', 'Independence', 'St. Joseph', 'St. Charles', 'Blue Springs'],
    'Montana': ['Billings', 'Missoula', 'Great Falls', 'Bozeman', 'Butte', 'Helena', 'Kalispell', 'Havre', 'Anaconda', 'Miles City'],
    'Nebraska': ['Omaha', 'Lincoln', 'Bellevue', 'Grand Island', 'Kearney', 'Fremont', 'Hastings', 'Norfolk', 'North Platte', 'Columbus'],
    'Nevada': ['Las Vegas', 'Henderson', 'Reno', 'North Las Vegas', 'Sparks', 'Carson City', 'Elko', 'Mesquite', 'Boulder City', 'Fernley'],
    'New Hampshire': ['Manchester', 'Nashua', 'Concord', 'Derry', 'Dover', 'Rochester', 'Salem', 'Merrimack', 'Keene', 'Hudson'],
    'New Jersey': ['Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Edison', 'Woodbridge', 'Lakewood', 'Toms River', 'Hamilton', 'Trenton'],
    'New Mexico': ['Albuquerque', 'Las Cruces', 'Rio Rancho', 'Santa Fe', 'Roswell', 'Farmington', 'Clovis', 'Hobbs', 'Alamogordo', 'Carlsbad'],
    'New York': ['New York', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse', 'Albany', 'New Rochelle', 'Mount Vernon', 'Schenectady', 'Utica'],
    'North Carolina': ['Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem', 'Fayetteville', 'Cary', 'Wilmington', 'High Point', 'Greenville'],
    'North Dakota': ['Fargo', 'Bismarck', 'Grand Forks', 'Minot', 'West Fargo', 'Williston', 'Dickinson', 'Mandan', 'Jamestown', 'Wahpeton'],
    'Ohio': ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron', 'Dayton', 'Parma', 'Canton', 'Youngstown', 'Lorain'],
    'Oklahoma': ['Oklahoma City', 'Tulsa', 'Norman', 'Broken Arrow', 'Lawton', 'Edmond', 'Moore', 'Midwest City', 'Enid', 'Stillwater'],
    'Oregon': ['Portland', 'Salem', 'Eugene', 'Gresham', 'Hillsboro', 'Beaverton', 'Bend', 'Medford', 'Springfield', 'Corvallis'],
    'Pennsylvania': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie', 'Reading', 'Scranton', 'Bethlehem', 'Lancaster', 'Harrisburg', 'Altoona'],
    'Rhode Island': ['Providence', 'Warwick', 'Cranston', 'Pawtucket', 'East Providence', 'Woonsocket', 'Coventry', 'Cumberland', 'North Providence', 'South Kingstown'],
    'South Carolina': ['Columbia', 'Charleston', 'North Charleston', 'Mount Pleasant', 'Rock Hill', 'Greenville', 'Summerville', 'Goose Creek', 'Hilton Head Island', 'Spartanburg'],
    'South Dakota': ['Sioux Falls', 'Rapid City', 'Aberdeen', 'Brookings', 'Watertown', 'Mitchell', 'Yankton', 'Pierre', 'Huron', 'Vermillion'],
    'Tennessee': ['Nashville', 'Memphis', 'Knoxville', 'Chattanooga', 'Clarksville', 'Murfreesboro', 'Franklin', 'Jackson', 'Johnson City', 'Bartlett'],
    'Texas': ['Houston', 'San Antonio', 'Dallas', 'Austin', 'Fort Worth', 'El Paso', 'Arlington', 'Corpus Christi', 'Plano', 'Laredo'],
    'Utah': ['Salt Lake City', 'West Valley City', 'Provo', 'West Jordan', 'Orem', 'Sandy', 'Ogden', 'St. George', 'Layton', 'Taylorsville'],
    'Vermont': ['Burlington', 'South Burlington', 'Rutland', 'Essex Junction', 'Colchester', 'Bennington', 'Brattleboro', 'Hartford', 'Milton', 'Barre'],
    'Virginia': ['Virginia Beach', 'Norfolk', 'Chesapeake', 'Arlington', 'Richmond', 'Newport News', 'Hampton', 'Alexandria', 'Portsmouth', 'Roanoke'],
    'Washington': ['Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue', 'Kent', 'Everett', 'Renton', 'Yakima', 'Spokane Valley'],
    'West Virginia': ['Charleston', 'Huntington', 'Parkersburg', 'Morgantown', 'Wheeling', 'Weirton', 'Fairmont', 'Beckley', 'Clarksburg', 'Martinsburg'],
    'Wisconsin': ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine', 'Appleton', 'Waukesha', 'Eau Claire', 'Oshkosh', 'Janesville'],
    'Wyoming': ['Cheyenne', 'Casper', 'Laramie', 'Gillette', 'Rock Springs', 'Sheridan', 'Green River', 'Evanston', 'Riverton', 'Cody']
}


# Define the main service categories
services_list = [
    "Auto Services",
    "Legal Services",
    "Medical Services",
    "Restaurants",
    "Beauty",
    "Home Services",
    "Insurance",
    "Pet Services"
    
]

# Define the other service lists
auto_services_list = [
    "Auto Body Shops",
    "Auto Glass Repair",
    "Auto Parts",
    "Auto Repair",
    "Car Detailing",
    "Oil Change",
    "Roadside Assistance",
    "Tire Shops",
    "Towing",
    "Window Tinting"
]

beauty_services_list = [
    "Barber Shops",
    "Beauty Salons",
    "Beauty Supplies",
    "Days Spas",
    "Facial Salons",
    "Hair Removal",
    "Hair Supplies",
    "Hair Stylists",
    "Massage",
    "Nail Salons"
]

home_services_list = [
    "AC Repair",
    "Appliance Repair",
    "Carpet Cleaning",
    "Electricians",
    "Garage Door Repair",
    "Moving Companies",
    "Pest Control Services",
    "Plumbers",
    "Self Storage"
]

insurance_list = [
    "Boat Insurance",
    "Business Insurance",
    "Car Insurance",
    "Dental Insurance",
    "Disability Insurance",
    "Flood Insurance",
    "Home Insurance",
    "Liability Insurance",
    "Life Insurance"
]

legal_services_list = [
    "Accounting Service",
    "charter accountant",
    "Attorneys",
    "Bail Bonds",
    "Bankruptcy Attorneys",
    "Car Accident Lawyer",
    "Divorce Attorneys",
    "Family Law Attorneys",
    "Lie Detector Tests",
    "Private Investigators",
    "Process Servers",
    "Stenographers",
    "Tax Attorneys"
]

medical_services_list = [
    "Dentists",
    "Dermatologists",
    "Doctors",
    "Endocrinologists",
    "Gynecologists",
    "Hospitals",
    "Neurologists",
    "Ophthalmologists",
    "Optometrists",
    "Physical Therapy",
    "Podiatrists"
]

pet_services_list = [
    "Animal Shelters",
    "Dog Training",
    "Doggy Daycares",
    "Emergency Vets",
    "Kennels",
    "Mobile Pet Grooming",
    "Pet Boarding",
    "Pet Cemeteries",
    "Pet Grooming"
]

restaurant_list = [
    "Breakfast Restaurants",
    "Chinese Restaurants",
    "Cuban Restaurants",
    "Italian Restaurants",
    "Korean Restaurants",
    "Mexican Restaurants",
    "Seafood Restaurants",
    "Sushi Bars",
    "Thai Restaurants",
    "Vegetarian Restaurants",
    "Pizza Parlors",
    "Fast Food Restaurants",
    "Steak Houses",
    "Family Style Restaurants",
    "Barbecue Restaurants",
    "Take Out Restaurants"
]

# Create a dictionary to map the categories to their respective lists
service_dictionary = {
    "Auto Services": auto_services_list,
    "Legal Services": legal_services_list,
    "Medical Services": medical_services_list,
    "Restaurants": restaurant_list,
    "Beauty": beauty_services_list,
    "Home Services": home_services_list,
    "Insurance": insurance_list,
    "Pet Services": pet_services_list
    
}


async def scrape_website(url,state,city,category):

    ua_path = pathlib.Path(__file__).parent/"ua.csv"
    df = pd.read_csv(ua_path)
    ua_list = list(df["ua"])
    custom_headers = {
        "authority": "www.facebook.com",
        "scheme": "https",
        "Accept": "/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        browser = await launch({
            'headless': True,
            'args': ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage','--start-maximized','--disable-infobars']
        })
        page = await browser.newPage()
        await page.setUserAgent(random.choice(ua_list))
        await stealth(page)
        await page.setViewport({'width': 1535, 'height': 1080})
        await page.setExtraHTTPHeaders(custom_headers)
        page.setDefaultNavigationTimeout(50000)


        await page.goto(url)
        await asyncio.sleep(10)

        while True:
            found_data = []

            companies = await page.querySelectorAll("div.search-results.organic > div.result")
            for company in companies:
                try:
                    business_name = await company.querySelectorEval("a.business-name > span","el => el.textContent")
                except:
                    business_name = "null"

                try:
                    business_url = await company.querySelectorEval("a.business-name","el => el.href")
                except:
                    business_url = "null"
                

                try:
                    website = await company.querySelectorEval("div.info-section.info-primary > div.links > a.track-visit-website","el => el.href")
                except:
                    website = "null"
                

                try:
                    phone = await company.querySelectorEval("div.info-section.info-secondary > div.phones.phone.primary","el => el.textContent")
                except:
                    phone = "null"
                
                item = {}
                item["name"] = str(business_name).strip()
                item["phone"] = str(phone).replace("(","").replace(")","")
                item["website"] = str(website).split("?")[0]
                item["link"] = str(business_url).strip()
                item["state"] = str(state)
                item["city"] = str(city)
                item["category"] = category
                found_data.append(item)
            
            save_data(found_data)

            try:   
                next_page = await page.xpath('//*[@id="main-content"]/div[3]/div[4]/ul/li[last()]/a')
                next_url = await page.evaluate("el => el.href",next_page[0])
                
                await page.goto(next_url)
                await asyncio.sleep(10)
            except:
                await browser.close()
                break
                

            
        

   
    except Exception as e:
        print(f"Error: {e}")




def save_data(items):
                 
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Connected to the database successfully.")

            # Create a cursor object to execute SQL queries
            cur = conn.cursor()

            try:
                # Construct the SQL query for data insertion and execute it
                for item in items:
                    query = f"INSERT INTO {TABLE_NAME} (name,phone,website,link,state,city,category) VALUES (%s, %s, %s,%s,%s,%s,%s);"

                    cur.execute(query,(item["name"],item["phone"],item["website"],item["link"],item["state"],item["city"],item["category"]))
                conn.commit()
            except Exception as e:
                conn.rollback()  # Rollback the transaction if an error occurs
                print("Error: Unable to insert data.")
                print(e)

        except Exception as e:
            print("Error: Unable to connect to the database.")
            print(e)

        finally:
            # Close the cursor and the database connection
            cur.close()
            conn.close()



async def starttask(urls):
    tasks = []
    for url in urls:
        tasks.append(scrape_website(url["url"],state=url["state"],city=url["city"],category=url["category"]))
        

    await asyncio.gather(*tasks)



# Main App

selected_service = services_list[1]
selected_subservice = service_dictionary[selected_service][0]
selected_state = us_states_full_names[4]
selected_cities = us_states_cities[selected_state][0:4]


if __name__=="__main__":
    state_abbr = us_state_abbreviations[selected_state]
    urls = []
    base_url = "https://www.yellowpages.com/search?"
    
    for city in selected_cities:
        params = {
        "search_terms": selected_subservice,
        "geo_location_terms": str(city)+f", {state_abbr}"
        }
        urls.append({"url": base_url+urlencode(params),"state":selected_state,"city": city,"category":selected_subservice})
    
    asyncio.get_event_loop().run_until_complete(starttask(urls))


    
        


