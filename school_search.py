import csv
import heapq
from collections import defaultdict
from dataclasses import dataclass
from typing import TextIO, Tuple, OrderedDict
from functools import partial
import difflib
import time


def _record_generator(csvfile: TextIO, skiplines: int = 1):
    csv_reader = csv.DictReader(csvfile, delimiter=',')
    while skiplines:
        next(csv_reader)
        skiplines -= 1
    for row in csv_reader:
        yield row


def _build_name_index(name_index: defaultdict, row: dict):
    # tokenize school name
    school_name: str = row.get('SCHNAM05')
    words = school_name.split()
    for word in words:
        name_index[word.lower()].append(row)


def _build_city_index(city_index: defaultdict, row: dict):
    city: str = row.get('LCITY05')
    words = city.split()
    for word in words:
        city_index[word.lower()].append(row)


_state_code_map = {
    'alabama': 'al',
    'alaska': 'ak',
    'arizona': 'az',
    'arkansas': 'ak',
    'california': 'ca',
    'colorado': 'co',
    'connecticut': 'ct',
    'delaware': 'de',
    'district of columbia': 'dc',
    'florida': 'fl',
    'georgia': 'ga',
    'hawaii': 'hi',
    'idaho': 'id',
    'illinois': 'il',
    'indiana': 'in',
    'iowa': 'ia',
    'kansas': 'ks',
    'kentucky': 'ky',
    'louisiana': 'la',
    'maine': 'me',
    'maryland': 'md',
    'massachusetts': 'ma',
    'michigan': 'mi',
    'minnesota': 'mn',
    'mississippi': 'ms',
    'missouri': 'mo',
    'montana': 'mt'
}

_reverse_state_code_map = {v: k for k, v in _state_code_map.items()}


def _build_state_index(state_index: defaultdict, row: dict):
    state_usps = row.get('LSTATE05')
    state_index[state_usps.lower()].append(row)
    state_index[_reverse_state_code_map.get(state_usps.lower())].append(row)


_name_index = defaultdict(list)
_city_index = defaultdict(list)
_state_index = defaultdict(list)


def init_indices(file_path: str = './school_data.csv', encoding='cp1252'):
    build_name_index = partial(_build_name_index, _name_index)
    build_city_index = partial(_build_city_index, _city_index)
    build_state_index = partial(_build_state_index, _state_index)

    with open(file_path, newline='', encoding=encoding) as csvfile:
        for record in _record_generator(csvfile):
            build_name_index(record)
            build_city_index(record)
            build_state_index(record)


@dataclass
class SearchResultItem:
    item: OrderedDict
    score: float


def search_schools(query: str, matches=3, score=0.6):
    """
    search for schools based on query string
    :param query: query object
    :param matches: number of top matches
    :param score: threshold below which approximate matches are discarded
    :return:
    """
    word_vector = query.split()
    results = {}
    start_time = time.time()
    for word in word_vector:
        for index in [_name_index, _city_index, _state_index]:
            similar_keys = difflib.get_close_matches(word, index.keys(), n=matches, cutoff=score)
            for key in similar_keys:
                record_list = index.get(key)
                for record in record_list:
                    if record.get('NCESSCH') in results:
                        record['NCESSCH'].score += score
                    else:
                        record['NCESSCH'] = SearchResultItem(item=record, score=score)
    stop_time = time.time()
    top_results = heapq.nlargest(matches, key=lambda r: r.score)

    print(f"Results for \"{query}\" (search took {stop_time - start_time} )")
    for i, res in enumerate(top_results, 1):
        school_name = res.item['SCHNAM05']
        school_city = res.item['LCITY05']
        school_state = res.item['LSTATE05']
        print(f"i: {school_name}\n{school_city}, {school_state}")
