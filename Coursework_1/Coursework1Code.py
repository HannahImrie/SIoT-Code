
import json
import time
import datetime
import requests
from bson import ObjectId, json_util
from pymongo import MongoClient
print("Establishing Connection...")

try:
    client = MongoClient("mongodb+srv://HannahUser:Table12House24Basic@hannah-iot-cluster.qwcik.mongodb.net/Hannah-IoT-Cluster?retryWrites=true&w=majority")
    print("Connection Successful...")
except:
    try:
        print("Could not connect to MongoDB...")
        print("Attempting again...")
    except:
        print("Can't connect :(")
 
# Only need to connect one as the time out limit is 30 mins and I am calling every 5
    
db = client.project1database    # Route to database on Mongo Server
collection = db.oneweekofdata   # Route to collection where the documents are stored
collection2 = db.updateabledocs


def parse_json(data): #parses the data for it is in the correct from (used to ensure " instead of ')
        return json.loads(json_util.dumps(data))


timeout = time.time() + 60 * 60 * 24 * 10 #running for 10 days worth of data collection


def main():
    count = 0
    while True:
        if time.time() > timeout: #if it goes beyond this 10 day time, break the while true loop
            break
        elif time.time() < timeout: 
            count = count+1
            print (count)
            Start = time.time()
            # Weather Data from Open Weather Map API
            # Test data for terminal in Ravenscourt Park Station, Hammersmith (300037)
            lat = 51.49 # taken from tfl website
            lon = -0.24 # taken from tfl website
            my_key = '755c70082a775db26c20327b0310c542' # API Key for OpenWeatherMap
            r = requests.get('http://api.openweathermap.org/data/2.5/weather?lat='+str(lat)+'&lon='+str(lon)+'&appid='+str(my_key)+'&units=metric')
            data = r.json() #store the data as a json
            
            #weather data below collected from OpenWeatherMap for London
            weather = data['weather'][0]['description']
            wind_speed = data['wind']['speed']
            temp = data['main']['temp']
            feels_like = data['main']['feels_like']
            visibility = data['visibility']
            humidity = data['main']['humidity']
            
            #Bike Data  from tfl API
            Bike_id = 668 #Terminal Id for Ravenscourt Park Station, Hammersmith (300037)
            My_key = 'e2f4519ee0864e1dabb44966c68e28fc' # API subscription key
            r = requests.get('https://api.tfl.gov.uk/BikePoint/Bikepoints_'+str(Bike_id)+'?app_key=' + My_key)
            data = r.json() #store the data as a json
            Bikes = data['additionalProperties'][6]['value'] # Number of available bikes
            Empty = data['additionalProperties'][7]['value'] # Number of available Docks
            now = datetime.datetime.now() # Current Time

            # creating JSON file to be sent to MongoDB
            dict = {
                "date":now.strftime("%d/%m/%Y"),
                "time" :now.strftime("%H:%M:%S"),
                "bikes" :Bikes,
                "empty" :Empty,
                "weather" :weather,
                "wind" :wind_speed,
                "temp" :temp,
                "feels" :feels_like,
                "visibility" :visibility,
                "humidity" :humidity
                }
            
            file_data = parse_json(dict)        # put the data in the correct form
            collection.insert_one(file_data)    # insert data in collection
            
            q = {"_id":ObjectId("5fff0a3d96b67af682c85c81")}    # id the databse assigned to the document
            file = collection2.find_one(q)                      # get the document from the updatebledocs collection
            
            # append each new value on the end of the list
            file["date"].append(now.strftime("%d/%m/%Y"))
            file["time"].append(now.strftime("%H:%M:%S"))
            file["bikes"].append(Bikes)
            file["empty"].append(Empty)
            file["weather"].append(weather)
            file["wind"].append(wind_speed)
            file["temp"].append(temp)
            file["feels"].append(feels_like)
            file["visibility"].append(visibility)
            file["humidity"].append(humidity)
            
            # remove the first value in the list to ensure exactly 24 points are stored
            for i in ["time","date","bikes","empty","weather","wind","temp","feels","visibility","humidity"]:
                    file[i].pop(0)
                    
            print (len(file["feels"]))
            file.pop("_id",None)                    # removes the Id object as the file won't parse with it in
            newfile = parse_json(file)              # put the data in the correct form  
            collection2.replace_one(q,newfile)      # replace the old file with the data from newfile
            
            End = time.time()
            timer = End-Start
            print ("Time Taken: ", timer)
            time.sleep(300-round(timer))        # Timer to wait 5 mins from start of loop
            
            
            
main()