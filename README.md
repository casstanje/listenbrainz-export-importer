# listenbrainz-export-importer
A python script to import your exported listenbrainz data, for example to make it easier to remove multiple listens by just exporting and deleting the data, editing the json files, and then reimporting

NOTE: This does NOT import loved tracks. Might add it later, but right now, it doesn't
## Dependencies
- Python
    - [liblistenbrainz](https://github.com/metabrainz/liblistenbrainz)
    - [validators](https://pypi.org/project/validators/)
    - [pandas](https://pandas.pydata.org/)
## Usage
```
$ python listenbrainz-importer.py -h
usage: listenbrainz-importer [-h] --token TOKEN exportPath

positional arguments:
  exportPath     path to the unzipped export folder from listenbrainz

options:
  -h, --help     show this help message and exit
  --token TOKEN  listenbrainz token
```
You can get your listenbrainz token from [here](https://listenbrainz.org/profile)

The export folder should look like this:
```
listenbrainz_user_id
    ├── feedback.jsonl
    ├── listens
    │   ├── year
    │   │   ├── index.jsonl
    │   │   ├── index.jsonl
    │   │   ├── index.jsonl
    │   │   └── index.jsonl
    │   └── year
    │       ├── index.jsonl
    │       ├── index.jsonl
    │       ├── index.jsonl
    │       ├── index.jsonl
    │       ├── index.jsonl
    │       └── ...
    └── user.json
```