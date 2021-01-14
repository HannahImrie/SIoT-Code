
import json
import time
import datetime
import requests
import numpy as np
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

# Only need to connect one as the time out limit is 30 mins and I am calling every 5 mins
    
db = client.project1database        # Route to database on Mongo Server
collection = db.updateabledocs      # Route to collection where the documents are stored

# lat and long for each weather zone 
w_list = [[51.46,-0.10],[51.46,-0.15],[51.46,-0.20],[51.48,-0.00],[51.48,-0.10],
          [51.48,-0.15],[51.48,-0.20],[51.48,-0.25],[51.50,-0.00],[51.50,-0.05],
          [51.50,-0.10],[51.50,-0.15],[51.50,-0.20],[51.50,-0.25],[51.52,-0.00],
          [51.52,-0.05],[51.52,-0.10],[51.52,-0.15],[51.52,-0.20],[51.52,-0.25], 
          [51.54,-0.00],[51.54,-0.05],[51.54,-0.10],[51.54,-0.15],[51.54,-0.20]]

def round_nearest(x,a):         # rounds x to the nearest a
  return round(round(x/a)*a ,2) # ie 5.677 to the nearest 0.02 = 5.68

def parse_json(data): #parses the data for it is in the correct from (used to ensure " instead of ')
        return json.loads(json_util.dumps(data))

q = {"_id":ObjectId("5ffcfb448de85120ed3dcd37")}  # id the databse assigned to the document
file = collection.find_one(q)             # get the document from the updatebledocs collection



def main(file):
    count = 0
    hour = 0
    dow = 0
    while True:

        start = time.time() # current time
        count = count + 1
        now = datetime.datetime.now()  #datetime onject created
        lastdow = dow                   # updating last day of the week
        dow = now.weekday()             # updating current day of the week
        lasthour = hour                 # updating last hour
        hour = now.hour             # updating curretn hour
        time_row = now.strftime("%Y/%m/%d")+" "+now.strftime("%H:%M:%S") #formating the time
        
        # variable and data lists
        weather_list = []
        wind_list = []
        temp_list = []
        feels_list = []
        visibility_list = []
        humidity_list = []
        b_data = []
        w_data = []
        
        print ('Calling APIs...')
        #Bike Data  from tfl API
        My_key = 'e2f4519ee0864e1dabb44966c68e28fc' # API subscription key
        r = requests.get('https://api.tfl.gov.uk/BikePoint?app_key=' + My_key)
        b_data = r.json()  #store the data as a json
        print ('Bike Data Received...')
        
        #Weather data from OpenWeatherMap API
        for i in w_list:  # getting data for each of the weather zones
            lat = i[0]
            lon = i[1]
                    
            my_key = '755c70082a775db26c20327b0310c542' # API Key for OpenWeatherMap
            r = requests.get('http://api.openweathermap.org/data/2.5/weather?lat='+str(lat)+'&lon='+str(lon)+'&appid='+str(my_key)+'&units=metric')
            temp_data = r.json() #store the data as a json
            w_data.append(temp_data) #append to a list of JSONs
        
        print ('Weather Data Received...') 
        # append the weather data assciated with each zone to the vraible lists
        for i in range(25):
            weather_list.append(w_data[i]['weather'][0]['description'])
            wind_list.append(w_data[i]['wind']['speed'])
            temp_list.append(w_data[i]['main']['temp'])
            feels_list.append(w_data[i]['main']['feels_like'])
            visibility_list.append(w_data[i]['visibility'])
            humidity_list.append(w_data[i]['main']['humidity']) 
        print("Weather data sorted...")

        for i in range(len(b_data)): #repeats for the number of bikes currently in service
            for j in range(len(file["features"])): # for every bikepoint if features
                if file["features"][j]["properties"]["id_name"] == b_data[i]["id"]: # if the BikePoint names match then correct bikepoint has been found continue
                    weather_index = w_list.index([round_nearest(b_data[i]["lat"],0.02),round_nearest(b_data[i]["lon"],0.05)]) # gets the index value for weather data
                    
                    if lasthour+1==hour or lasthour-23 ==hour:  # if it has just become a new hour 
                        thing = file["features"][j]["properties"]["bike"] # collect the last hours bike data
                        mean =  np.array(thing).astype(np.float) # turn it into a np array
                        mean = np.mean(mean)  # calculate the mean
                        oldmean = file["features"][j]["properties"]["activity"][lastdow][lasthour] #get the old mean
                        file["features"][j]["properties"]["activity"][lastdow][lasthour] = int(round((mean+oldmean)*0.5,0)) #add them togther and half it then round to an integer
                    
                    try:
                        # if the bike/dock availability data is less than 50 and a valid file type append to file else skip
                        if int(b_data[i]["additionalProperties"][6]["value"]) < 50 and int(b_data[i]["additionalProperties"][7]["value"]) < 50:
                            file["features"][j]["properties"]["bike"].append(b_data[i]["additionalProperties"][6]["value"])
                            file["features"][j]["properties"]["bike"].pop(0) # remove first element in list
                            file["features"][j]["properties"]["empty"].append(b_data[i]["additionalProperties"][7]["value"])
                            file["features"][j]["properties"]["empty"].pop(0) 
                    except: # for errro handling
                        print(b_data[i]["id"])
                        print(b_data[i]["additionalProperties"][6]["value"])
                        print(b_data[i]["additionalProperties"][7]["value"])
                        print("Bad respose")
                        continue

                    # append the new data and remove the first element to ensure only 1 hour of data is stored
                    file["features"][j]["properties"]["weather"].append(weather_list[weather_index])
                    file["features"][j]["properties"]["weather"].pop(0)
                    
                    file["features"][j]["properties"]["wind"].append(wind_list[weather_index])
                    file["features"][j]["properties"]["wind"].pop(0)
                    
                    file["features"][j]["properties"]["temp"].append(temp_list[weather_index])
                    file["features"][j]["properties"]["temp"].pop(0)
                    
                    file["features"][j]["properties"]["feels"].append(feels_list[weather_index])
                    file["features"][j]["properties"]["feels"].pop(0)
                    
                    file["features"][j]["properties"]["visibility"].append(visibility_list[weather_index])
                    file["features"][j]["properties"]["visibility"].pop(0)
                    
                    file["features"][j]["properties"]["humidity"].append(humidity_list[weather_index])
                    file["features"][j]["properties"]["humidity"].pop(0)
                    
                    file["features"][j]["properties"]["datetime"].append(time_row)
                    file["features"][j]["properties"]["datetime"].pop(0)
                    
                    
                    combined = '\t'.join(file["features"][j]["properties"]["weather"])  # combine all the strings in the weather list
                    if "rain"  in combined:                         # which makes it easier to identify if a string is in it
                        file["features"][j]["properties"]["rain"] = "yes" # if it rained print yes
                    elif "drizzle" in combined:
                        file["features"][j]["properties"]["rain"] = "yes"
                    else:
                        file["features"][j]["properties"]["rain"] = "no" #if it didn't print no

        file.pop("_id",None)        # removes the Id object as the file won't parse with it in
        newfile = parse_json(file)   # put the data in the correct form  
        collection.replace_one(q,newfile) # replace the old file with the data from newfile
        
        end = time.time()
        timer = end-start
        time.sleep(300-timer)       # Timer to wait 5 mins from start of loop
main(file) # call main function
