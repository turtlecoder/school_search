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
    'montana': 'mt',
    'nebraska': 'ne',
    'nevada': 'nv',
    'new hampshire': 'nh',
    'new york': 'ny',
    'new jersey': 'nj',
    'new mexico': 'nm',
    'north carolina': 'nc',
    'north dakota': 'nd',
    'ohio': 'oh',
    'oklahoma': 'ok',
    'oregon': 'or',
    'pennsylvania': 'pa',
    'peurto rico': 'pr',
    'rhode island': 'ri',
    'south carolina': 'sc',
    'south dakota': 'sd',
    'tennessee': 'tn',
    'texas': 'tx',
    'utah': 'ut',
    'vermont': 'vt',
    'virginia': 'va',
    'virgin islands': 'vi',
    'washington': 'wa',
    'west virginia': 'wi',
    'wisconsin': 'wi',
    'wyoming': 'wy'

}

_reverse_state_code_map = {v: k for k, v in _state_code_map.items()}


def _build_state_index(state_index: defaultdict, row: dict):
    state_usps = row.get('LSTATE05')
    state_index[state_usps.lower()].append(row)
    state_name = _reverse_state_code_map.get(state_usps.lower())
    # if its a valid state name, then append it
    if state_name:
        state_index[state_name].append(row)


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


def search_schools(query: str, matches=5, score=1):
    """
    search for schools based on query string
    :param query: query object
    :param matches: number of top matches
    :param score: threshold below which approximate matches are discarded
    :return:
    """
    word_vector = query.split()
    results = {}
    start_time = time.clock()
    for word in word_vector:
        for weight, index in [(3, _name_index), (2, _city_index), (1, _state_index)]:
            similar_keys = difflib.get_close_matches(word.lower(), index.keys(), n=matches, cutoff=score)
            for key in similar_keys:
                record_list = index.get(key)
                for record in record_list:
                    result_key = record.get('NCESSCH')
                    if result_key in results:
                        results[result_key].score += weight
                    else:
                        results[result_key] = SearchResultItem(item=record, score=weight)
    stop_time = time.clock()
    top_results = heapq.nlargest(matches, results.values(), key=lambda r: r.score)
    print(f"Results for \"{query}\" (search took {stop_time - start_time}s )")
    for i, res in enumerate(top_results, 1):
        school_name = res.item['SCHNAM05']
        school_city = res.item['LCITY05']
        school_state = res.item['LSTATE05']
        print(f"{i}: {school_name}\n{school_city}, {school_state}")


init_indices()
