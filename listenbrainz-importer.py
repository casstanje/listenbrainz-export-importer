import liblistenbrainz
import time
import json
import sys
import logging
import os
import argparse
import validators
import pandas as pd
from datetime import datetime, timezone
from dateutil import tz

parser = argparse.ArgumentParser("listenbrainz-importer")
parser.add_argument('--token', dest='token', type=str, help='listenbrainz token', required=True)
parser.add_argument('--url', dest='apiUrl', type=str, help='listenbrainz api url (default: https://api.listenbrainz.org)', default="https://api.listenbrainz.org")
parser.add_argument("exportPath", help="path to the unzipped export folder from listenbrainz", type=str)
args = parser.parse_args()


if args.exportPath is not None and args.token is not None:
    apiUrl = args.apiUrl
    path = args.exportPath
    token = args.token
    client = liblistenbrainz.ListenBrainz()
    try:
        client.set_auth_token(token)
    except:
        logging.exception("Invalid user token")
        sys.exit()
    if validators.url(apiUrl):
        if os.path.isdir(path) and os.path.exists(path):
            totalListens = []
            for dir in os.walk(path + "/listens"):
                for file in os.listdir(dir[0]):
                    if os.path.splitext(file)[1] == ".jsonl":
                        with open(dir[0] + "/" + file) as f:
                            print("\nParsing " + dir[0] + "/" + file)
                            fileObject = pd.read_json(path_or_buf=f, lines=True)
                            listens = []
                            for index, track in fileObject.iterrows():
                                trackData = track['track_metadata']
                                utcDateTime = track['listened_at'].to_pydatetime()
                                utcDateTime = utcDateTime.replace(tzinfo=tz.tzutc())
                                actualDateTime = utcDateTime.astimezone(tz.tzlocal())
                                releaseName = None
                                try:
                                    trackData["release_name"]
                                except:
                                    print("No release name for " + trackData["track_name"])
                                listen = liblistenbrainz.Listen(
                                    track_name = trackData['track_name'],
                                    artist_name = trackData['artist_name'],
                                    release_name = releaseName,
                                    listened_at = str(int(actualDateTime.timestamp()))
                                )
                                listens.append(listen)
                                totalListens.append(listen)
                            print("Last track parsed: \n" + "listened at (seconds since epoch): " + listens[-1].listened_at + "\ntrack title: " + listens[-1].track_name + "\nartist name: " + listens[-1].artist_name)
            print("\nTotal Listens: " + str(len(totalListens)))
            i = 0
            successScrobbles = 0
            failedScrobbles = 0
            smallerLists = [totalListens[i:i + 25] for i in range(0, len(totalListens), 25)]
            while i < len(smallerLists):
                try:
                    if client.remaining_requests < 5: time.sleep(client.ratelimit_reset_in)
                    client.submit_multiple_listens(smallerLists[i])
                    successScrobbles += len(smallerLists[i])
                except:
                    logging.exception("")
                    print("Couldn't scrobble all listens in group. Scrobbling them individually instead")
                    # Submit listens individually if multiple fails
                    individualIndex = 0
                    while individualIndex < len(smallerLists[i]):
                        if client.remaining_requests < 5: time.sleep(client.ratelimit_reset_in)
                        try:
                            client.submit_single_listen(smallerLists[i][individualIndex])
                            successScrobbles += 1
                        except:
                            print("Couldn't scrobble " + smallerLists[i][individualIndex].track_name)
                            failedScrobbles += 1
                        individualIndex += 1
                print("\nSucceeded scrobbles: " + str(successScrobbles) + "\nFailed scrobbles: " + str(failedScrobbles) + "\n" + str(len(totalListens) - successScrobbles - failedScrobbles) + " scrobbles to go")
                i += 1
                    
            
            print("Done!")
        else:
            logging.exception("Provided path is either not a folder or dosen't exist")
            sys.exit()
    else:
        logging.exception("API url is not valid")
        sys.exit()

else:
    logging.exception("Missing arguments. Run python 'listenbrainz-import.py -h' for help")
    exit