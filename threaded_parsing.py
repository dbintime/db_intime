import urllib.request
import urllib.parse
from time import sleep

import requests
import pymongo
from pymongo.collection import Collection
from pymongo.results import UpdateResult
import json
import ast
import threading


API_KEY = 'CQv024gGLhn95I06cU1QruZyQZVfej9R3211'

username = "xxx"
passwort = "yyy"
client = pymongo.MongoClient(f"mongodb+srv://{username}:{passwort}@cluster0.85krh.azure.mongodb.net/db?retryWrites=true&w=majority")
headers = {
        'DB-Api-Key': API_KEY,
        'Cookie': 'TS015a6fe4=0121ca1b9579397db5b6076b7b14b2cf812d1606e18e47344a7861dc0636c42e29ba104be98a095d71c4fdc2433f7bdee5065fd721'
    }

threads = list()


def request_api(station_eva, thread_number=-1):
    payload = {}

    url = 'https://gateway.businesshub.deutschebahn.com/ris-boardspublic/trial/v1/public/arrivals/{}'.format(station_eva)
    response = requests.request("GET", url, headers=headers, data=payload)

    arrivals: Collection = client.db["arrivals"]
    try:
        arr= response.json()["arrivals"]
        for entry in arr:

            update_result: UpdateResult = arrivals.replace_one({"arrivalID": entry["arrivalID"]}, entry, upsert=True)
            if update_result.matched_count > 0:
                print(".", end="")
            else:
                print("!", end="")

    except Exception as inst:
        try:
            if 'rate limit exceeded' in str(response.content):
                print("Rate Limit exceeded ... waiting 5s (thread #{})".format(thread_number), flush=True)
                sleep(5)
                request_api(station_eva, thread_number)
        except Exception as inst2:
            print("Exception occured: {}".format(inst2))


def req(station_ava,thread_number=-1, loop=False):
    if loop:
        print("looped request to {}".format(station_ava))
        while (True):
            request_api(station_ava,thread_number)
            print("Sleep 30s")
            sleep(30)
            print("next iteration")
    else:
        request_api(station_ava,thread_number)


def thread_function(subList, thread_number):
    sleep(thread_number+1)
    print("thread {} started with stations: {}".format(thread_number, subList), flush=True)
    while True:
        print("next iteration for thread {}\n".format(thread_number), flush=True)
        processed = 0
        for station in subList:
            req(station,thread_number)
            processed = processed + 1
            print("request to {} (thread #{}: {}/{})".format(station,thread_number,processed,len(subList)), flush=True)
            sleep(1)
        print("Loop completed for thread {}, sleeping 60s\n".format(thread_number), flush=True)
        sleep(60)


def start_threaded_processing(subList,thread_number):
    print("creating thread {}  with stations: {}".format(thread_number,subList), flush=True)
    x = threading.Thread(target=thread_function, args=(subList,thread_number,))
    threads.append(x)
    x.start()


if __name__ == '__main__':

    stations = []
    with open("stations.txt") as f:
        for line in f:

            correct_line = str(line.strip()).replace("'", '"')
            try:
                entry = json.loads(correct_line)
                if 'HIGH_SPEED_TRAIN' in entry['availableTransports'] or 'INTERCITY_TRAIN' in entry['availableTransports'] or 'INTER_REGIONAL_TRAIN' in entry['availableTransports'] or 'REGIONAL_TRAIN' in entry['availableTransports'] or 'CITY_TRAIN' in entry['availableTransports']:
                    stations.append(entry['evaNumber'])
            except Exception as inst:
                print("Exception occured on parsing json for string {}: {}".format(entry,inst))
    print("found large stations: {}".format(len(stations)))
    stationCount = 0
    subList = []
    threadCount = 0
    for station in stations:
        subList.append(station)
        stationCount = stationCount+1
        if stationCount % 400 == 0:
            start_threaded_processing(subList, threadCount)
            threadCount = threadCount + 1
            subList=[]
    start_threaded_processing(subList, threadCount)


