from json import loads, dumps
from json.decoder import JSONDecodeError
from time import sleep
from csv import DictReader, Sniffer
from os.path import join, basename, exists, isdir, isfile, splitext, split as path_split
from os import listdir, makedirs, walk, system
from zipfile import is_zipfile, ZipFile
from gzip import open as gzip_open
from tarfile import is_tarfile, open as tarfile_open 
from bz2 import decompress
from codecs import BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE
from shutil import copy, rmtree as delete_dir_tree
from typing import Union, List, Optional, Tuple, Set, Dict, DefaultDict, Any
from sys import argv
from re import search, match
from random import random
import logging
import requests
try:
    import PyPDF2
    pdf_is_suported = True
except:
    pdf_is_suported = False

log = logging.getLogger(__name__)

VERSION        = '0.0.2'

UNZIP_DIR      = 'unzipDir'
DATA_DIR       = 'dataDir'
LOG_DIR        = 'log'

HTTP_VERBS     = ['POST', 'GET', 'PUT', 'PATCH', 'DELETE']
JOB_TYPES      = ['PROCESSING', 'CLUSTERING', 'SAMPLING', 'CLASSIFICATION']
JOB_STATES     = ['RUNNING', 'SUCCEEDED']
FIELD_TYPES    = ['TEXT_EN', 'KEYWORD', 'ENTITY', 'EMAIL_ADDRESS', 'INTEGER',
                  'DOUBLE', 'BOOLEAN', 'DATE', 'PATH']
FIELD_ROLES    = ['EMAIL_SUBJECT', 'EMAIL_BODY', 'EMAIL_SENT_DATE',
                  'EMAIL_SENDER', 'EMAIL_TO', 'EMAIL_CC', 'EMAIL_BCC',
                  'EMAIL_ATTACHMENT', 'EMAIL_CONVERSATION_INDEX',
                  'EMAIL_HEADEREXTRACTION', 'DEDUPLICATION', 'CLUSTERING',
                  'HIGHLIGHTING', 'FAST_HIGHLIGHTING']
COL_TRANS      = ['DELETE', 'RECOVER', 'COMMIT', 'PROCESS']
COL_STATES     = ['EMPTY', 'IMPORTING', 'COMMITTING', 'COMMITTED', 'PROCESSING',
                  'PROCESSED', 'ABORTED', 'DELETING']
CLA_TRANS      = ['TRAIN']
CLA_STATES     = ['INITIALIZED', 'TRAINING', 'TRAINED', 'INVALID']

DOCUMENT_TYPES   = ['email', 'default', 'autodetect']
PREPROCESS_TYPES = ['rfc822', 'NUIX', 'IPRO']

_COL_APPEAR    = 'appear'
_COL_DISAPPEAR = 'disappear'
_COL_EVENTS    = [_COL_APPEAR, _COL_DISAPPEAR]
_ITEM_TYPES    = ['collections', 'classifiers', 'jobs:jobinstances']

MAX_BATCH_SIZE = 1000
MAX_ATTEMPTS   = 1

DEL_ALL_COL        = "delete_all_collections"
DEL_COL            = "delete_collection"
CREATE_MISSING_COL = "create_collection_if_not_exists"
RECREATE_COL       = "recreate_collection"
NO_ACTION          = "no_action"
PRE_ACTIONS        = [NO_ACTION, DEL_ALL_COL, DEL_COL, RECREATE_COL, CREATE_MISSING_COL]

AUTO           = "auto"
AYFIE          = "ayfie"
AYFIE_RESULT   = "ayfie_result"
CSV            = "csv"
JSON           = "json"
PDF            = "pdf"
WORD_DOC       = "docx"
XLSX           = "xlsx"
TXT            = "txt"
XML            = 'xml'
DATA_TYPES     = [AYFIE, AYFIE_RESULT, JSON, CSV, PDF, TXT]
JSON_TYPES     = [AYFIE, AYFIE_RESULT, JSON]
ZIP_FILE       = 'zip'
TAR_FILE       = 'tar'
GZ_FILE        = 'gz'
BZ2_FILE       = 'bz2'
_7Z_FILE       = '7z'
RAR_FILE       = 'rar'
ZIP_FILE_TYPES = [ZIP_FILE, TAR_FILE, GZ_FILE, BZ2_FILE, _7Z_FILE, RAR_FILE]
TEXT_ENCODINGS = [BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE]

FILE_SIGNATURES = {
    b'\x1f\x8b': GZ_FILE,
    b'\x42\x5a\x68': BZ2_FILE,
    b'\x37\x7A\xBC\xAF\x27\x1C': _7Z_FILE,
    b'\x52\x61\x72\x21\x1A\x07': RAR_FILE,
    b'\x25\x50\x44\x46': PDF,
    b'\x3c\x3f\x78\x6d\x6c\x20': XML,
    BOM_UTF8: BOM_UTF8,
    BOM_UTF16_LE: BOM_UTF16_LE,
    BOM_UTF16_BE: BOM_UTF16_BE,
    BOM_UTF32_LE: BOM_UTF32_LE,
    BOM_UTF32_BE: BOM_UTF32_BE
}    
BOM_MARKS_NAMES = {
    BOM_UTF8: 'utf-8',
    BOM_UTF16_LE: 'utf_16_le',
    BOM_UTF16_BE: 'utf_16_be',
    BOM_UTF32_LE: 'utf_32_le',
    BOM_UTF32_BE: 'utf_32_be'
}

MAX_CONNECTION_RETRIES = 100
MAX_PAUSE_BEFORE_RETRY = 120

class HTTPError(IOError):
    pass

class ConfigError(Exception):
    pass

class AutoDetectionError(Exception):
    pass

class DataFormatError(Exception):
    pass

def pretty_print(json):
    if type(json) is str:
        if not len(json):
            raise ValueError('json string cannot be empty string')
        json = loads(json)
    print(dumps(json, indent=4))
    
def getNextFile(root, dir_filter=None):
    items = listdir(root)
    files = [item for item in items if isfile(join(root, item))]
    dirs = [item for item in items if isdir(join(root, item))]
    for f in files:
        yield join(root, f)
    for dir in dirs:
        if dir_filter:
            if dir in dir_filter:
                continue
        for f in getNextFile(join(root, dir), dir_filter):
            yield f


class Ayfie:

    def __init__(self, server='127.0.0.1', port='80', version='v1'):
        self.server = server
        self.port = port
        self.version = version
        self.product = 'ayfie'
        self.headers = {'Content-Type': 'application/hal+json'}
        self.statusCodes = self.__getHTTPStatusCodes()

    def __getHTTPStatusCodes(self):
        return {
            200: {
                "code": "200 OK",
                "description": "The request completed successfully"
            },
            201: {
                "code": "201 Created",
                "description": "A new resource has been created successfully"
            },
            204: {
                "code": "204 No Content",
                "description": "An update to an existing resource has " +
                               "been applied successfully"
            },
            400: {
                "code": "400 Bad Request",
                "description": "The request was malformed"
            },
            404: {
                "code": "404 Not Found",
                "description": "The requested resource did not exist"
            },
            405: {
                "code": "405 Method Not Allowed",
                "description": "This one is not listed in ayfie literature."
            },
            409: {
                "code": "409 Conflict",
                "description": "This one is not listed in ayfie literature."
            },
            500: {
                "code": "500 Internal Server Error",
                "description": "The request cannot be completed " +
                               "successfully due to a server error."
            },
            502: {
                "code": "502 Bad Gateway",
                "description": "This one is not listed in ayfie literature."
            },
            504: {
                "code": "504 Gateway Time-out",
                "description": "This one is not listed in ayfie literature."
            }
        }

    def __validateInputValue(self, input_value, allowed_values):
        if input_value not in allowed_values:
            allowed_values_str = '", "'.join(allowed_values)
            error_msg = f'Unknown input value "{input_value}".'
            error_msg += f'Allowed values are: "{allowed_values_str}".'
            raise ValueError(error_msg)

    def __get_endpoint(self, path):
        return f'http://{self.server}:{self.port}/{self.product}/' +\
               f'{self.version}/{path}'

    def __gen_req_log_msg(self, verb, endpoint, data, headers):
        if len(str(data)) > 1000:
            data = {"too much":"data to log"}
        return f"Request: {verb} {endpoint} json={data} headers={headers}"

    def __gen_response_err_msg(self, response_obj, ayfieStatus):
        ayfieStatusCode = ayfieStatus['code']
        ayfieDescription = ayfieStatus['description']
        err_msg = "No error message found within response object"
        if 'error' in response_obj:
            err_msg = response_obj['error']
        elif 'errors' in response_obj:
            es = response_obj['errors']
            err_msg = "; ".join([f"{e['documentId']}:{e['message']}" for e in es]) 
        msg = f'HTTP error code: "{ayfieStatusCode}". '
        msg += f'General error description: "{ayfieDescription}". '
        msg += f'Specific error description: "{err_msg}"'
        return msg

    def __gen_non_ayfie_code_msg(self, code):
        ayfieCodes = ', '.join(list(map(str, self.statusCodes.keys())))
        return f"Unexpected HTTP status code {code}, Expected: {ayfieCodes}"

    def __get_id_from_header(self, headers):
        if 'location' in headers:
            url = headers['location']
            p = f"^https?://.*/ayfie/{self.version}/(collections|jobs)/(.*$)"
            m = match(p, url)
            if m:
                return m.group(2)
        return None

    def __execute(self, path, verb, data={}):
        collection_state_reporting = False
        attempts = 0
        while True:
            attempts += 1
            try:
                if collection_state_reporting:
                    m = match(r"^collections/([^/]+)($|/)", path)   
                    if m:
                        col_id = m.group(1)
                        state = self.__interact(f'collections/{col_id}', 'GET')["collectionState"]
                        log.debug(f"Collection {col_id} now (before) in state {state}")
                result = self.__interact(path, verb, data)
                if collection_state_reporting:
                    if m:
                        try:
                            state = self.__interact(f'collections/{col_id}', 'GET')["collectionState"]
                            log.debug(f"Collection {col_id} now (after) in state {state}")
                        except:
                            log.debug(f"Unable to obtain after-state for collection {col_id}")
                break
            except Exception as e:
                if attempts > MAX_ATTEMPTS:
                    raise
                log.error(f"Connection error: {str(e)}")
                sleep_period = attempts * 10
                if sleep_period > 60:
                    sleep_period = 60
                sleep(sleep_period)
        return result
            
    def __interact(self, path, verb, data={}):
        self.__validateInputValue(verb, HTTP_VERBS)
        request_function = getattr(requests, verb.lower())
        endpoint = self.__get_endpoint(path)
        log.debug(self.__gen_req_log_msg(verb, endpoint, data, self.headers))
        if data and verb == "POST" and "batches" in endpoint:
            with open("data.json", 'wb') as f:
                f.write(dumps(data).encode('utf-8'))
        tries = 0
        while True:
            if tries > MAX_CONNECTION_RETRIES:
                break
            tries += 1
            try:
                response = request_function(endpoint, json=data, headers=self.headers)
                break
            except requests.exceptions.ConnectionError as e:
                sleep_interval = tries * 5
                if sleep_interval > MAX_PAUSE_BEFORE_RETRY:
                    sleep_interval = MAX_PAUSE_BEFORE_RETRY 
                log.debug(f"Connection issue: {str(e)}. Trying again in {sleep_interval} seconds")
                sleep(sleep_interval)
            except Exception as e:
                print(f"Previous unknown Connection issue that has to be handled in the future: {str(e)}")  
                raise
        id_from_headers = self.__get_id_from_header(response.headers)
        log.debug(f'HTTP response status: {response.status_code}')
        if response.status_code in self.statusCodes.keys():
            ayfieStatus = self.statusCodes[response.status_code]
        else:
            ayfieStatus = {
                "code": f"{response.status_code}",
                "description": "Not a ayfie recognized response code"
            }
            log.warning(self.__gen_non_ayfie_code_msg(response.status_code))
        try:
            response_obj = loads(response.text) if len(response.text) else {}
        except JSONDecodeError:
            pattern = "<head><title>(\d{3,3}) (.*)</title></head>"
            m = search(pattern, response.text)
            if m:
                response_obj = {'error': m.group(2)}
            else:
                raise HTTPError(response.text)
        except:
            raise HTTPError(response.text)
        if response.status_code >= 400:
            error_msg = self.__gen_response_err_msg(response_obj, ayfieStatus)
            log.debug(error_msg)
            if response.status_code >= 500:
                with open(f"failed-batch-{response.status_code}-error.json", "wb") as f:
                    f.write(dumps(data).encode('iso-8859-1'))
            raise HTTPError(error_msg)
        if id_from_headers:
            return id_from_headers
        return response_obj

    def __wait_for_collection_event(self, col_id, event, timeout):
        self.__validateInputValue(event, _COL_EVENTS)
        count_down = timeout
        while (True):
            if self.exists_collection_with_id(col_id):
                if event == _COL_APPEAR:
                    return
            else:
                if event == _COL_DISAPPEAR:
                    return
            if count_down < 0:
                negation = " not" if event == _COL_APPEAR else " "
                msg = f'Collection {col_id} still{negation} there '
                msg += f'after {timeout} seconds'
                raise TimeoutError(msg)
            sleep(5)
            count_down -= 1

    def __wait_for_collection_to_appear(self, col_id, timeout=120):
        self.__wait_for_collection_event(col_id, _COL_APPEAR, timeout)

    def __wait_for_collection_to_disappear(self, col_id, timeout=120):
        self.__wait_for_collection_event(col_id, _COL_DISAPPEAR, timeout)

    def __change_collection_state(self, col_id, transition):
        self.__validateInputValue(transition, COL_TRANS)
        data = {"collectionState": transition}
        return self.__execute(f'collections/{col_id}', 'PATCH', data)

    def __wait_for_collection_state(self, col_id, state, timeout):
        self.__validateInputValue(state, COL_STATES)
        count_down = timeout
        while (True):
            current_state = self.get_collection_state(col_id)
            if current_state == state:
                return
            if count_down < 0:
                err_msg = f'Collection {col_id} still in state '
                err_msg += f'{current_state} and not in {state} after '
                err_msg += f'{timeout} seconds'
                raise TimeoutError(err_msg)
            sleep(5)
            count_down -= 1
 
    def __wait_for_collection_to_be_committed(self, col_id, timeout):
        self.__wait_for_collection_state(col_id, 'COMMITTED', timeout)

    def __wait_for_collection_to_be_processed(self, col_id, timeout):
        self.__wait_for_collection_state(col_id, 'PROCESSED', timeout)
        
    def __get_all_items_of_an_item_type(self, item_type):
        self.__validateInputValue(item_type, _ITEM_TYPES)
        itemNames = item_type.split(':')
        endpoint = itemNames[0]
        resultField = itemNames[1] if len(itemNames) > 1 else endpoint
        result = self.__execute(endpoint, 'GET')
        if '_embedded' in result:
            if resultField in result['_embedded']:
                return result['_embedded'][resultField]
        return []
        
    def __wait_for_job_state(self, job_id, state, timeout):
        self.__validateInputValue(state, JOB_STATES)
        count_down = timeout
        while (True):
            current_state = self.get_job_state(job_id)
            if current_state == state:
                return
            if count_down < 0:
                err_msg = f'Jobv {job_id} still in state '
                err_msg += f'{current_state} and not in {state} after '
                err_msg += f'{timeout} seconds'
                raise TimeoutError(err_msg)
            sleep(30)
            count_down -= 1

    ######### Collection Management #########

    def create_collection(self, col_name, col_id=None):
        data = {'name': col_name}
        if col_id:
            data['id'] = col_id
        return self.__execute('collections', 'POST', data)

    def create_collection_and_wait(self, col_name, col_id=None, timeout=120):
        col_id = self.create_collection(col_name, col_id)
        self.__wait_for_collection_to_appear(col_id, timeout)
        return col_id
        
    def get_collections(self):
        return self.__get_all_items_of_an_item_type('collections')   

    def delete_collection(self, col_id):
        self.__execute(f'collections/{col_id}', 'DELETE')

    def delete_collection_and_wait(self, col_id, timeout=120):
        self.delete_collection(col_id)
        self.__wait_for_collection_to_disappear(col_id, timeout)

    def delete_all_collections_and_wait(self, timeout=120):
        col_ids = self.get_collection_ids()
        for col_id in col_ids:
            self.delete_collection(col_id)
        for col_id in col_ids:
            self.__wait_for_collection_to_disappear(col_id, timeout)

    def get_collection(self, col_id):
        return self.__execute(f'collections/{col_id}', 'GET')

    def get_collection_state(self, col_id):
        return self.get_collection(col_id)["collectionState"]

    def get_collection_schema(self, col_id):
        return self.__execute(f'collections/{col_id}/schema', 'GET')
        
    def add_collection_schema_field_dict(self, col_id, field_schema):
        path = f'collections/{col_id}/schema/fields'
        return self.__execute(path, 'POST', field_schema)

    def add_collection_schema_field(self, col_id, field_name, type,
                                    is_list=True, roles=None):
        self.__validateInputValue(type, FIELD_TYPES)
        if roles:
            for role in roles:
                self.__validateInputValue(role, FIELD_ROLES)
        field_schema = {"name": field_name, "type": type,
                "list": is_list, "roles": roles}
        add_collection_schema_field_dict(col_id, field_schema)
        
    def get_collection_schema_field(self, col_id, field_name):
        path = f'collections/{col_id}/schema/fields/{field_name}'
        return self.__execute(path, 'GET')
        
    def exists_collection_schema_field(self, col_id, field_name):
        schema = self.__execute(f'collections/{col_id}/schema', 'GET')
        if field_name in schema["fields"]:
            return True
        return False

    def commit_collection(self, col_id):
        self.__change_collection_state(col_id, "COMMIT")

    def process_collection(self, col_id, config=None):
        self.__change_collection_state(col_id, "PROCESS")

    def train_collection(self, classifier_name):
        # Need to be merge with collection transitions
        data = {"classifierState": 'TRAIN'}
        return self.__execute(f'classifiers/{classifier_name}', 'PATCH', data)

    def commit_collection_and_wait(self, col_id, timeout=300):
        self.commit_collection(col_id)
        self.__wait_for_collection_to_be_committed(col_id, timeout)

    def process_collection_and_wait(self, col_id, config=None, timeout=300):
        self.process_collection(col_id, config)
        self.__wait_for_collection_to_be_processed(col_id, timeout)

    def train_collection_and_wait(self, col_id, timeout=300):
        self.train_collection(col_id)
        self.__wait_for_collection_to_be_trained(col_id, timeout)

    def feed_collection_documents(self, col_id, documents):
        path = f'collections/{col_id}/batches'
        data = {'documents': documents}
        response = self.__execute(path, 'POST', data)
        return response

    def get_collection_documents(self, col_id, page_size=100):
        path = f'collections/{col_id}/documents?size={page_size}'
        return self.__execute(path, 'GET')

    def get_collection_size(self, col_id):
        path = f'collections/{col_id}/documents?size=0'
        return self.__execute(path, 'GET')["meta"]["totalDocuments"]

    def __get_collections_item(self, item, col_name=None):
        return [collection[item] for collection in self.get_collections()
                if not col_name or collection['name'] == col_name]
        
    def get_collection_names(self):
        return self.__get_collections_item('name')

    def get_collection_ids(self, col_name=None):
        return self.__get_collections_item('id', col_name)

    def get_collection_id(self, col_name):
        ids = self.get_collection_ids(col_name)
        if len(ids) > 1:
            raise ConfigError(f'More than one collection named "{col_name}"')
        if len(ids) == 1:
            return ids[0]
        return None

    def exists_collection_with_id(self, col_id):
        if col_id in self.get_collection_ids():
            return True
        return False

    def exists_collection_with_name(self, col_name):
        if col_name in self.get_collection_names():
            return True
        return False

    def feed_and_process_collection_and_wait(self, col_id, documents):
        self.feed_collection_documents(col_id, documents)
        self.commit_collection_and_wait(col_id, timeout=300)
        self.process_collection_and_wait(col_id, timeout=300)

    def json_based_meta_search(self, col_id, json_query):
        path = f'collections/{col_id}/search'
        return self.__execute(path, 'POST', json_query)

    def json_based_meta_concept_search(self, col_id, json_query):
        path = f'collections/{col_id}/search/concept'
        return self.__execute(path, 'POST', json_query)

    def json_based_doc_search(self, col_id, json_query):
        return self.json_based_meta_search(col_id, json_query)['result']

    def search_collection(self, col_id, query, size=10, offset=0,
                          filters=[], exclude=[]):
        json_query = {
            "highlight" : False,
            "sort": {
                "criterion": "_score",
                "order": "desc"
            },
            "minScore": 0.0,
            "exclude": exclude,
            "aggregations": ["_cluster"],
            "filters": [],
            "query": query,
            "scroll": False,
            "size": size,
            "offset": 0
        }
        return self.json_based_doc_search(col_id, json_query)

    def get_search_result_doc_ids(self, col_id, query, size=1000):
        result = self.search_collection(col_id, query, size)
        return [doc['document']['id'] for doc in result]

    ######### Jobs #########

    def create_job(self, job_config):
        self.__execute('jobs', 'POST', job_config)

    def create_job_and_wait(self, job_config):
        self.create_job(job_config)
        sleep(120)  # temp placeholder until state checker implemented

    def get_jobs(self):
        return self.__get_all_items_of_an_item_type('jobs:jobinstances')


    ######### Clustering #########

    def __get_clustering_config(self, col_id, clusters_per_level=20,
                              docs_per_cluster=1000, max_recursion_depth=2,
                              gramian_singular_vectors=50, min_df=3,
                              max_df_fraction=0.25, gramian_threshold=0.14,
                              filters=[], output_field="_cluster"):
        return {
            "collectionId" : col_id,
            "type" : "CLUSTERING",
            "settings" : {
                "clustering" : {
                    "clustersPerLevel" : clusters_per_level,
                    "documentsPerCluster" : docs_per_cluster,
                    "maxRecursionDepth" : max_recursion_depth,
                    "gramianSingularVectors" : gramian_singular_vectors,
                    "minDf" : min_df,
                    "maxDfFraction" : max_df_fraction,
                    "gramianThreshold" : 0.14
                }
            },
            "filters" : filters,
            "outputField" : output_field
        }

    def create_clustering_job(self, col_id, **kwargs):
        config = self.__get_clustering_config(col_id, **kwargs)
        self.create_job(config)
        
    def create_clustering_job_and_wait(self, col_id, **kwargs):
        config = self.__get_clustering_config(col_id, **kwargs)
        self.create_job_and_wait(config)
        

    ######### Classification #########

    def create_classifier(self, classifier_name, col_id, training_field,
                          min_score, num_results):
        classifier_id = classifier_name  # For now at least
        data = {
            "name": classifier_name,
            "id": classifier_id,
            "collectionId": col_id,
            "trainingClassesField": training_field,
            "minScore": min_score,
            "numResults": num_results
        }
        self.__execute('classifiers', 'POST', data)
        
    def get_classifiers(self):
        return self.__get_all_items_of_an_item_type('classifiers')

    def get_classifier(self, classifier_name):
        return self.__execute(f'classifiers/{classifier_name}', 'GET')

    def get_classifier_names(self):
        return [classifier['name'] for classifier in self.get_classifiers()]

    def delete_all_classifiers(self):
        classifier_names = self.get_classifier_names()
        for classifier_name in classifier_names:
            self.delete_classifier(classifier_name)

    def delete_classifier(self, classifier_name):
        return self.__execute(f'classifiers/{classifier_name}', 'DELETE')

    def tag_document(self, col_id, doc_id, tag_field, tag_values):
        data = {tag_field: tag_values}
        endpoint = f'collections/{col_id}/documents/{doc_id}'
        self.__execute(endpoint, 'PATCH', data)

    def tag_documents(self, col_id, doc_ids, tag_field, tag_values):
        for doc_id in doc_ids:
            self.tag_document(col_id, doc_id, tag_field, tag_values)  

    def create_classifier_job_and_wait(self, classifier_name, col_id, min_score,
                                       num_results, filters, output_field):
        job_config = {
            "collectionId" : col_id,
            "type" : "CLASSIFICATION",
            "settings" : {
                "classification" : {
                    "classifierId" : classifier_name,
                    "minScore" : min_score,
                    "numResults" : num_results
                }
            },
            "filters" : filters,
            "outputField" : output_field
        }
        self.create_job_and_wait(job_config)
        

class DataSource():

    def __init__(self, source_path, config, data_dir=None,
                                            unzip_dir=UNZIP_DIR):
        self.created_data_dir = False
        if not exists(source_path):
            raise ValueError(f"The path '{source_path}' does not exists")
        if isdir(source_path):
            self.data_dir = source_path
        elif isfile(source_path):
            if data_dir:
                self.data_dir = data_dir
            else:
                self.data_dir = DATA_DIR + '_' +  str(int(random() * 10000000))
                self.created_data_dir = True
            self.__recreate_directory(self.data_dir)
            copy(source_path, self.data_dir)
        else:
            msg = f'Source path "{source_path}" is neither a directory, '
            msg += 'a regular file nor a supported zip file type.'
            raise ValueError(msg)
        self.config = config
        self.unzip_dir = unzip_dir

    def __del__(self):
        if self.created_data_dir:
            delete_dir_tree(self.data_dir)

    def __get_text_format_type(self, start_byte_sequence, extension):
        start_char_sequence = start_byte_sequence.decode('iso-8859-1')
        for char in [' ', '\n', '\r', '\t']:
            start_char_sequence = start_char_sequence.replace(char, '')
        char_patterns = {
            '{"documents":[{': 'ayfie',
            '{"query":{': 'ayfie_result'
        }
        for pattern in char_patterns.keys():
            if start_char_sequence.startswith(pattern):
                return char_patterns[pattern]
            
        if start_char_sequence.startswith('{') and extension == '.json':
            return JSON
        elif extension == '.txt':
            return TXT
        elif extension == '.csv' or extension == '.tsv':
            with open(file_path, 'r') as f:
                try:
                    dialect = Sniffer().sniff(f.read(1024))
                    return CSV
                except:
                    return None
            
    def __get_file_type(self, file_path):
        self.detected_encoding = None
        extension = splitext(file_path)[-1].lower()
        if is_zipfile(file_path):
            if extension == ".docx":
                return WORD_DOC
            elif extension == ".xlsx":
                return XLSX
            return ZIP_FILE
        elif is_tarfile(file_path):
            return TAR_FILE
        else:
            with open(file_path, 'rb') as f:
                start_byte_sequence = f.read(100)
            for pattern in FILE_SIGNATURES.keys():
                if start_byte_sequence.startswith(pattern):
                    if pattern in TEXT_ENCODINGS:
                        self.detected_encoding = BOM_MARKS_NAMES[pattern]
                        log.debug(f"'{file_path}' encoding auto detected to '{self.detected_encoding}'")
                        start_byte_sequence = start_byte_sequence[len(pattern):]
                        return self.__get_text_format_type(start_byte_sequence, extension)
                    return FILE_SIGNATURES[pattern]
            return self.__get_text_format_type(start_byte_sequence, extension)
        return None

    def __unzip(self, file_type, file_path):
        if file_type not in ZIP_FILE_TYPES:
            return False
        self.__recreate_directory(self.unzip_dir)
        if file_type == ZIP_FILE:
            with ZipFile(file_path, 'r') as z:
                z.extractall(self.unzip_dir)
        elif file_type == TAR_FILE:
            with tarfile_open(file_path) as tar:
                tar.extractall(self.unzip_dir)
        elif file_type == GZ_FILE:
            with open(output_path, 'wb') as f, gzip_open(file_path, 'rb') as z:
                f.write(z.read())
        elif file_type == BZ2_FILE:
            with open(output_path, 'wb') as f, open(file_path,'rb') as z:
                f.write(decompress(z.read()))
        elif file_type == _7Z_FILE:
            log.info(f"Skipping '{file_path}' - 7z decompression still not implemented")
        elif file_type == RAR_FILE:
            log.info(f"Skipping '{file_path}' - rar decompression still not implemented")
        else:
            raise ValueError('Unknown zip file type "{zip_file_type}"')
        return True

    def __convert_pdf_to_text(self, file_path):
        if not pdf_is_suported:
            raise ModuleNotFoundError('run "pip install PyPDF2"')
        with open(file_path, 'rb') as f:
            pdf = PyPDF2.PdfFileReader(f)
            pages = []
            for page_number in range(pdf.getNumPages()):
                pages.append(pdf.getPage(page_number).extractText())
            return ' '.join(pages)

    def __get_file_content(self, file, file_path=None):
        if type(file) is str:
            file_path = file
            with open(file_path, 'rb') as f:
                f = self.__skip_file_byte_order_mark(f)
                content = f.read()
        else:
            f = self.__skip_file_byte_order_mark(file)
            content = f.read()
        if self.config.encoding == AUTO:
            if self.detected_encoding:
                try:
                    return content.decode(self.detected_encoding)
                except UnicodeDecodeError:
                    msg = f"Auto detected encoding {self.detected_encoding} failed for {file_path}, "
                    try:
                        content = content.decode('utf-8')
                        log.debug(msg + "using 'utf-8' instead")
                    except UnicodeDecodeError:
                        content = content.decode('iso-8859-1')
                        log.debug(msg + "using 'iso-8859-1' instead")
                    return content
            else:
                msg = f"No encoding was successfully auto detected for {file_path}, "
                try:
                    content = content.decode('utf-8')
                    log.debug(msg + "using 'utf-8' as fallback")
                except UnicodeDecodeError:
                    content = content.decode('iso-8859-1')
                    log.debug(msg + "using 'iso-8859-1' as fallback, after first trying 'utf-8' failed")
                return content
        else:
            return content.decode(self.config.encoding)
        raise Exception(f"Unable to resolve encoding for '{file_path}'")
            
    def __recreate_directory(self, dir_path):
        tries = 0
        while exists(dir_path):
            if tries > 5:
                raise IOError(f"Failed to delete directory '{dir_path}'")    
            delete_dir_tree(dir_path)
            tries += 1
            sleep(5 * tries)
        makedirs(dir_path)
        if not exists(dir_path):
            raise IOError("Failed to create directory '{dir_path}'")
        
    def __skip_file_byte_order_mark(self, f):
        bytes_to_skip = 0
        start_byte_sequence = f.read(4)
        for bom in TEXT_ENCODINGS:
            if start_byte_sequence[:len(bom)] == bom:
                bytes_to_skip = len(bom)
                break
        f.seek(0)
        f.read(bytes_to_skip)
        return f

    def __gen_id_from_file_path(self, file_path):
        return splitext(basename(file_path))[0]

    def __construct_doc(self, id, content):
        doc = {
            "id": id,
            "fields": {
                "content": content
            }
        }
        if self.config.second_content_field:
            doc["fields"][self.config.second_content_field] = content
        if self.config.document_type:
            if self.config.document_type in DOCUMENT_TYPES:
                doc["fields"]['documentType'] = self.config.document_type
            else:
                raise ConfigError(f"Unknown document type {document_type}")
        if self.config.preprocess:
            if self.config.preprocess in PREPROCESS_TYPES:
                doc["fields"]['preprocess'] = self.config.preprocess
            else:
                raise ConfigError(f"Unknown preprocess type {preprocess}")
        return doc

    def __get_document(self, file_path, auto_detected_file_type,
                                        is_training_doc=False):
        retrieve_document = True
        if self.config.file_picking_list:
            retrieve_document = False
            filename = basename(file_path)
            if filename in self.config.file_picking_list or f"{splitext(filename)[0]}" in self.config.file_picking_list:
                if self.config.file_destination: 
                    if not exists(self.config.file_destination):
                        makedirs(self.config.file_destination)                
                    copy(file_path, self.config.file_destination)
                    retrieve_document = True
                    
        if not retrieve_document:
            return None
            
        data_type = self.config.data_type
        if data_type == AUTO:
            data_type = auto_detected_file_type
            if not data_type:
                log.error(f'File type detetection failed for "{file_path}"')
                return
        args = { 'mode': 'rb' }
        if data_type == CSV:       
            args = {
                'mode': 'r',
                'newline': '',
                'encoding': self.config.encoding
            }
        with open(file_path, **args) as f:
            if data_type == CSV:
                mappings = self.config.csv_mappings
                for row in DictReader(f, **self.config.format):
                    if self.config.csv_mappings:
                        if is_training_doc:
                            training_value = row[mappings['training_column']].strip()
                            if training_value:
                                yield self.__construct_doc(row[mappings['id']],
                                                           training_value)
                        else:
                            yield self.__construct_doc(row[mappings['id']],
                                        row[mappings['fields']['content']])
                    else:
                        raise ConfigError("There is no csv mapping table")
            elif data_type in [AYFIE, AYFIE_RESULT]:
                data = self.__get_file_content(f, file_path)
                if data_type == AYFIE:
                    self.config.format['root'] = 'documents'
                elif data_type == AYFIE_RESULT:
                    self.config.format['root'] = 'result'
                try:
                    for document in loads(data)[self.config.format['root']]:
                        if data_type == AYFIE_RESULT:
                            yield self.__construct_doc(document["document"]["id"],
                                                  document["document"]["content"])
                        else:
                            yield document
                except JSONDecodeError as e:
                    raise DataFormatError(f"{file_path}: {str(e)}")  
            elif data_type == PDF:
                yield self.__construct_doc(self.__gen_id_from_file_path(file_path),
                                             self.__convert_pdf_to_text(file_path))
            elif data_type in [WORD_DOC, XLSX, XML]:
                log.info(f"Skipping '{file_path}' - conversion of {data_type} still not implemented")
            elif data_type == TXT:
                yield self.__construct_doc(self.__gen_id_from_file_path(file_path),
                                          self.__get_file_content(file_path))
            else:
                log.error(f"Unknown data type '{data_type}'")         

    def get_documents(self, is_training_doc=False):
        for file_path in getNextFile(self.data_dir):
            file_type = self.__get_file_type(file_path)
            log.debug(f"'{file_path}' type auto detected to '{file_type}'")
            if file_type in ZIP_FILE_TYPES:
                self.__unzip(file_type, file_path)
                for unzipped_file in getNextFile(self.unzip_dir):
                    file_type = self.__get_file_type(unzipped_file)
                    if file_type in ZIP_FILE_TYPES:
                        log.info(f"Skipping '{unzipped_file}' - zip files within zip files are not supported")
                        continue
                    for file in self.__get_document(unzipped_file, file_type,
                                                    is_training_doc):
                      if file:
                        yield file
            else:
                for file in self.__get_document(file_path, file_type,
                                                is_training_doc):
                    if file:
                        yield file


class AyfieConnector():
    
    def __init__(self, config, data_source=None, additional_data_source=None):
        self.data_source = data_source
        self.additional_data_source = additional_data_source
        self.config = config
        args = [self.config.server, self.config.port, self.config.api_version]
        self.ayfie = Ayfie(*args)
        
        
class SchemaManager(AyfieConnector):

    def add_field_if_not_there(self):
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        field_name = self.config.schema['name']
        schema = self.config.schema
        if schema["mode"] == 'override' or self.ayfie.exists_collection_schema_field(col_id, field_name):
            del schema["mode"]
            self.ayfie.add_collection_schema_field_dict(col_id, schema)
      
class Feeder(AyfieConnector):

    def collection_exists(self):
        if self.ayfie.exists_collection_with_name(self.config.col_name):
            return True
        else:
            return False

    def delete_collection_if_exists(self):
        if self.ayfie.exists_collection_with_name(self.config.col_name):
            log.info(f'Deleting collection "{self.config.col_name}"')
            col_id = self.ayfie.get_collection_id(self.config.col_name)
            self.ayfie.delete_collection_and_wait(col_id)

    def delete_all_collections(self):
        log.info('Deleting all collections')
        self.ayfie.delete_all_collections_and_wait()

    def create_collection_if_not_exists(self):
        if not self.ayfie.exists_collection_with_name(self.config.col_name):
            log.info(f'Creating collection "{self.config.col_name}"')
            col_id = self.config.col_name
            self.ayfie.create_collection_and_wait(self.config.col_name, col_id)

    def __send_batch(self):
        if self.config.report_doc_ids:
            print([doc['id'] for doc in self.batch])
        if self.config.ayfie_json_dump_file:
            with open(self.config.ayfie_json_dump_file, "wb") as f:
                f.write(dumps({'documents': self.batch}, indent=4).encode('utf-8'))
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        log.info(f'Feeding batch of {len(self.batch)} documents')
        self.ayfie.feed_collection_documents(col_id, self.batch)
        
    def feed_documents(self):
        self.batch = []
        self.doc_count = 0
        if not self.data_source:
            log.warning('No data source assigned')
            return
        for document in self.data_source.get_documents():
            if not self.config.no_feeding:
                self.batch.append(document)
                if len(self.batch) >= self.config.batch_size:
                    self.__send_batch()
                    self.doc_count += len(self.batch)
                    self.batch = []
                    if not self.config.silent_mode:
                        print(f"{self.doc_count} docs uploaded to collection '{self.config.col_name}' so far")
        if len(self.batch):
            self.__send_batch()
            self.doc_count += len(self.batch)
            if not self.config.silent_mode:
                print(f"A total of {self.doc_count} docs has now been uploaded to collection '{self.config.col_name}'")
        else:
            if self.config.no_feeding:
                print(f"Feeding turn off by no_feeding set to true")
            
    def feed_documents_commit_and_process(self):
        self.feed_documents()
        if self.doc_count > 0:
            col_id = self.ayfie.get_collection_id(self.config.col_name)
            log.info(f'Fed {self.doc_count} documents. Starting to commit')
            self.ayfie.commit_collection_and_wait(col_id)
            log.info(f'Done committing. Starting to process')
            if self.config.processing:
                job_config = {
                    "collectionId" : col_id,
                    "type" : "PROCESSING",
                    "settings" : self.config.processing
                }
                self.ayfie.create_job_and_wait(job_config) 
            else:
                self.ayfie.process_collection_and_wait(col_id)
            log.info(f'Done processing')
        else:
            log.info(f'Fed no documents')


class Classifier(AyfieConnector):

    def __create_classifier(self):
        if self.config.classifier_name in self.ayfie.get_classifier_names():
            self.ayfie.delete_classifier(self.config.classifier_name)
        self.ayfie.create_classifier(self.config.classifier_name,
                                     self.col_id,
                                     self.config.training_field,
                                     self.config.min_score,
                                     self.config.num_results)

    def __tag_documents_and_train_collection_and_wait(self):
        if self.config.training_set:  
            self.ayfie.tag_documents(self.col_id,
                                     self.config.training_set,
                                     self.config.training_field,
                                     ['tagged'])
        else:
            batch = []
            for document in self.training_source.get_documents(True):
                self.ayfie.tag_document(self.col_id,
                                        document['id'],
                                        self.config.training_field,
                                        [document['training']])
            self.ayfie.train_collection(self.config.classifier_name)
        sleep(120) # temp until state checker added

    def __create_classifier_job_and_wait(self):
        print(self.config.classifier_name,
              self.config.col_name,
              self.config.min_score,
              self.config.num_results,
              self.config.classifier_filters,
              self.config.classifier_output_field)       
        self.ayfie.create_classifier_job_and_wait(
                                self.config.classifier_name,
                                self.config.col_name,
                                self.config.min_score,
                                self.config.num_results,
                                self.config.classifier_filters,
                                self.config.classifier_output_field)

    def __calculate_accuracy(self):
        indexed_documents = {}
        result = self.ayfie.search_collection(self.col_id, "*", 1000,
                            exclude=["content", "term", "location", "organization", "person"])
        pretty_print(result)
        for indexed_document in result:
            doc = indexed_document['document']
            if 'pathclassification' in doc:
                if len(doc['pathclassification']) == 1:
                    category = doc['pathclassification'][0].split(':')[0][1:]
                else:
                    category = "both?"
            else:
                category = "none"
            indexed_documents[doc['id']] = category
        print(f"Doc id\tReality\tPrediction")
        for source_document in self.test_source.get_documents(True):
            if source_document['training'] == 'Yes':
                reality = 'pet'
            elif source_document['training'] == 'No':
                reality = 'wild'
            else:
                reality = "unknown"
            print(f"{source_document['id']}\t{reality}\t{indexed_documents[source_document['id']]}")

    def do_classification(self):
        self.training_source = self.data_source
        self.test_source = self.additional_data_source
        self.col_id = self.ayfie.get_collection_id(self.config.col_name)
        print('create classifier')
        self.__create_classifier()
        print('tag training docs')
        self.__tag_documents_and_train_collection_and_wait()
        print('run train classifier job')
        self.__create_classifier_job_and_wait()
        print('calculate accuracy')
        self.__calculate_accuracy()
        

class Clusterer(AyfieConnector):

    def create_clustering_job_and_wait(self):
        if self.config.clustering:
            col_id = self.ayfie.get_collection_id(self.config.col_name)
            cluster_config = {
                "collectionId" : col_id,
                "type" : "CLUSTERING",
                "settings":{
                    "clustering": self.config.clustering
                },
                "filters" : self.config.filters,
                "outputField" : self.config.clustering_output_field
            }
            log.info('Starting clustering job')  # TEMP Solution
            pretty_print(cluster_config)
            self.ayfie.create_job_and_wait(cluster_config)
        else:
            log.info('No clustering job')

            
class Querier(AyfieConnector):

    def search_collection(self):
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        search_query = self.config.search
        if self.config.search_query_file:
            with open(self.config.search_query_file, 'rb') as f:
                search_query = loads(f.read().decode('utf-8'))
        if (
                self.config.search_threshold or
                self.config.search_example or
                'threshold' in search_query or
                'example' in search_query):     
            return self.ayfie.json_based_meta_concept_search(col_id, search_query)
        return self.ayfie.json_based_meta_search(col_id, search_query)

    def get_collection_schema(self):
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        return self.ayfie.get_collection_schema(col_id)
 
 
class Config():

    def __init__(self, config):
        self.silent_mode     = self.__get_item(config, 'silent_mode', True)
        self.config_source   = self.__get_item(config, 'config_source', None)
        self.server          = self.__get_item(config, 'server', '127.0.0.1')
        self.port            = self.__get_item(config, 'port', '80')
        self.api_version     = self.__get_item(config, 'api_version', 'v1')
        self.col_name        = self.__get_item(config, 'col_name', None)
        self.csv_mappings    = self.__get_item(config, 'csv_mappings', None)
        self.schema          = self.__get_item(config, 'schema', False)
        self.__init_schema(self.schema)
        self.feeding         = self.__get_item(config, 'feeding', False)
        self.__init_feeder(self.feeding)
        self.processing      = self.__get_item(config, 'processing', False)
        self.__init_processing(self.processing)
        self.clustering      = self.__get_item(config, 'clustering', False)
        self.__init_clustering(self.clustering)
        self.classification  = self.__get_item(config, 'classification', False)
        self.__init_classification(self.classification)
        self.search          = self.__get_item(config, 'search', False)
        self.__init_search(self.search)
        self.regression_testing = self.__get_item(config, 'regression_testing', False)
        self.__init_regression_testing(self.regression_testing)
        
        self.__inputDataValidation()
        """
        if self.training_file:
            with open(self.training_file, 'rb') as f:
                self.training_set = [id.strip() for id in f.read().decode('utf8').split('\n')]
        else:
            self.training_set = None
        """
        self.training_set = None # instead of block above

    def __str__(self):
        varDict = vars(self)
        if varDict:
            return dumps(vars(self), indent=4)
        else:
            return None

    def __get_item(self, config, item, default):
        if config:
            if item in config and (config[item] or type(config[item]) is bool):
                return config[item]
        return default

    def get_operation_specific_col_name(self, operation, col_name):
        op_specific_col_name = self.__get_item(operation, 'col_name', None)
        if not op_specific_col_name:
            op_specific_col_name = col_name
        if not op_specific_col_name:
            raise ValueError("The feeding collection has to be set at some level in the config file")
        return op_specific_col_name
        
    def __init_schema(self, schema):
        if not schema:
            return
        s = {}
        s['mode']                     = self.__get_item(schema, 'mode', "add_if_not_exists")
        s['name']                     = self.__get_item(schema, 'name', None)
        s['type']                     = self.__get_item(schema, 'type', "TEXT")
        s['list']                     = self.__get_item(schema, 'list', False)
        s['roles']                    = self.__get_item(schema, 'roles', [])
        s['properties']               = self.__get_item(schema, 'properties', {})
        self.schema                   = s
        if not self.schema['name']:
            raise ConfigError('Field schema configurarion requires field name to be set')
 
    def __init_feeder(self, feeding):
        self.csv_mappings             = self.__get_item(feeding, 'csv_mappings', self.csv_mappings)
        self.data_type                = self.__get_item(feeding, 'data_type', AUTO)
        self.prefeeding_action        = self.__get_item(feeding, 'prefeeding_action', NO_ACTION)
        self.format                   = self.__get_item(feeding, 'format', {})
        self.encoding                 = self.__get_item(feeding, 'encoding', AUTO)
        self.data_file                = self.__get_item(feeding, 'data_file', None)
        self.data_dir                 = self.__get_item(feeding, 'data_dir', None)
        self.batch_size               = self.__get_item(feeding, 'batch_size', MAX_BATCH_SIZE)
        self.document_type            = self.__get_item(feeding, 'document_type', None)
        self.preprocess               = self.__get_item(feeding, 'preprocess', None)
        self.ayfie_json_dump_file     = self.__get_item(feeding, 'ayfie_json_dump_file', None)
        self.report_doc_ids           = self.__get_item(feeding, 'report_doc_ids', False)
        self.second_content_field     = self.__get_item(feeding, 'second_content_field', None)
        self.file_picking_list        = self.__get_item(feeding, 'file_picking_list', None)
        self.file_destination         = self.__get_item(feeding, 'file_destination', None)
        self.no_feeding               = self.__get_item(feeding, 'no_feeding', False)
        
    def __init_processing(self, processing):
        self.thread_min_chunks_overlap= self.__get_item(processing, 'thread_min_chunks_overlap', None)
        self.near_duplicate_evaluation= self.__get_item(processing, 'near_duplicate_evaluation', None)

    def __init_clustering(self, clustering):
        if not clustering:
            return
        c = {}
        c['clustersPerLevel']         = self.__get_item(clustering, 'clustersPerLevel', 20)
        c['documentsPerCluster']      = self.__get_item(clustering, 'documentsPerCluster', 1000)
        c['maxRecursionDepth']        = self.__get_item(clustering, 'maxRecursionDepth', 2)
        c['gramianSingularVectors']   = self.__get_item(clustering, 'gramianSingularVectors', 50)
        c['minDf']                    = self.__get_item(clustering, 'minDf', 3)
        c['maxDfFraction']            = self.__get_item(clustering, 'maxDfFraction', 0.25)
        c['gramianThreshold']         = self.__get_item(clustering, 'gramianThreshold', 0.14)
        self.clustering               = c
        self.filters                  = self.__get_item(clustering, 'filters', [])
        self.clustering_output_field  = self.__get_item(clustering, 'outputField', '_cluster')

    def __init_classification(self, classification):
        self.csv_mappings             = self.__get_item(classification, 'csv_mappings', self.csv_mappings)
        self.classifier_name          = self.__get_item(classification, 'classifier_name', None)
        self.training_column          = self.__get_item(classification, 'training_column', None)
        self.training_file            = self.__get_item(classification, 'training_file', None)
        self.test_file                = self.__get_item(classification, 'test_file', None)
        self.training_field           = self.__get_item(classification, 'training_field', 'trainingClass')
        self.min_score                = self.__get_item(classification, 'min_score', 0.6)
        self.num_results              = self.__get_item(classification, 'num_results', 1)
        self.classifier_filters       = self.__get_item(classification, 'filters', [])
        self.classifier_output_field  = self.__get_item(classification, 'classifier_output_field', 'pathclassification')

    def __init_search(self, search):
        self.search_query_file        = self.__get_item(search, 'search_query_file', None)
        self.search_result            = self.__get_item(search, 'result', "display")
        self.search_highlight         = self.__get_item(search, 'highlight', True),
        self.search_sort_criterion    = self.__get_item(search, 'sort_criterion', "_score")
        self.search_sort_order        = self.__get_item(search, 'sort_order', "desc")
        self.search_minScore          = self.__get_item(search, 'minScore', 0.0)
        self.search_exclude           = self.__get_item(search, 'exclude', [])
        self.search_aggregations      = self.__get_item(search, 'aggregations', [])
        self.search_filters           = self.__get_item(search, 'filters', [])
        self.search_query             = self.__get_item(search, 'query', "*")
        self.search_scroll            = self.__get_item(search, 'scroll', False)
        self.search_size              = self.__get_item(search, 'size', 10)
        self.search_offset            = self.__get_item(search, 'offset', 0)
        self.search_threshold         = self.__get_item(search, 'threshold', None)
        self.search_example           = self.__get_item(search, 'search_example', {})
        
    def __init_regression_testing(self, regression_testing):
        self.upload_config_dir        = self.__get_item(regression_testing, 'upload_config_dir', None)
        self.query_config_dir         = self.__get_item(regression_testing, 'query_config_dir', None)
        self.server_settings          = self.__get_item(regression_testing, 'server_settings', "always_override")

    def __inputDataValidation(self):
        if not self.regression_testing:
            if not self.col_name:
                raise ConfigError('Mandatory input parameter collection name (col_name) has not been given')    
        if self.data_file and self.data_dir:
            raise ConfigError('Can only set data_file or data_dir, not both')

            
class TestExecutor():

    def __init__(self, config, use_master_config_settings=True):
        self.config = config
        self.use_master_config_settings = use_master_config_settings
        
    def __execute_config(self, config_file_path):
        with open(config_file_path, 'rb') as f:
            try:
                config = loads(f.read().decode('utf-8'))
            except JSONDecodeError as e:
                print(f"ERROR: JSON syntax error in {config_file_path}: {str(e)}")
                return
        config['config_source'] = config_file_path
        if 'server' not in config or self.use_master_config_settings:
            config['server'] = self.config.server
        if 'port' not in config or self.use_master_config_settings:
            config['port'] = self.config.port

        if "feeding" in config:
            if "data_dir" in config["feeding"]:
                config["feeding"]['data_dir'] = self.__getFilePath(
                              config_file_path, config["feeding"]['data_dir'])
            if "data_file" in config:
                config["feeding"]['data_file'] = self.__getFilePath(
                              config_file_path, config["feeding"]['data_file'])

        if "search" in config:
            if "search_query_file" in config["search"]:
                config["search"]['search_query_file'] = self.__getFilePath(
                        config_file_path, config["search"]['search_query_file'])

        temp_config_file = 'tmp.cfg'
        with open(temp_config_file, 'wb') as f:
            f.write(dumps(config).encode('utf-8'))
        try:
            run_cmd_line(['ayfieExpress.py', temp_config_file])
        except Exception as e:
            print(f'TEST ERROR: "{config_file_path}": {str(e)}')
            raise

    def __retrieve_configs_and_execute(self, root_dir):
        for config_file_path in getNextFile(root_dir, ["data", "query", "disabled"]):
            self.__execute_config(config_file_path)

    def __getFilePath(self, root_path, leaf_path):
        if leaf_path.startswith('$/'):   
            return join(path_split(root_path)[0], leaf_path[2:])
        else:
            return leaf_path

    def run_tests(self):
        if self.config.upload_config_dir:
            config_dir = self.__getFilePath(self.config.config_source,
                                            self.upload_config_dir)
            self.__retrieve_configs_and_execute(config_dir)
        if self.config.query_config_dir:
            config_dir = self.__getFilePath(self.config.config_source,
                                            self.config.query_config_dir)
            self.__retrieve_configs_and_execute(config_dir)

    def process_test_result(self, result):
        if not self.config.config_source:
            self.config.config_source = "The test"
        if type(self.config.search_result) is bool:
            if self.config.search_result:
                print(f'CORRECT: {self.config.config_source} is hardcoded to be correct')
            else:
                print(f'FAILURE: {self.config.config_source} is hardcoded to be a failure')
        elif type(self.config.search_result) is int:
            expected_results = int(self.config.search_result)
            actual_results = int(result['meta']['totalDocuments'])
            if actual_results == expected_results:
                print(f'CORRECT: {self.config.config_source} returns {actual_results} results')
            else:
                print(f'FAILURE: {self.config.config_source} should return {expected_results} results, not {actual_results}')


class Admin():

    def __init__(self, config):
        log.info('######### Starting a new session #########')
        self.config = config
        source = None
        if self.config.data_file:
            source = self.config.data_file
        elif self.config.data_dir:
            source = self.config.data_dir
        if source:
            data_source = DataSource(source, self.config)
        else:
            data_source = None
        self.schema_manager = SchemaManager(self.config)
        self.feeder = Feeder(self.config, data_source)
        self.clusterer  = Clusterer(self.config)
        if self.config.training_file:
            training_source = DataSource(self.config.training_file, self.config)
        else:
            training_source = None
        if self.config.test_file:
            test_source = DataSource(self.config.test_file, self.config)
        else:
            test_source = None
        self.classifier = Classifier(self.config, training_source, test_source)
        self.querier = Querier(self.config)
        
    def run_config(self):
        if self.config.regression_testing:
            if self.config.server_settings == "always_override":
                use_master_settings = True
            else:
                use_master_settings = False
            testExecutor = TestExecutor(self.config, use_master_settings)
            testExecutor.run_tests()
            return
        
        if self.config.data_file or self.config.data_dir:
            if self.config.prefeeding_action not in PRE_ACTIONS:
                actions = '", "'.join(PRE_ACTIONS)
                msg = f'"prefeeding_action" must be one of these: "{actions}"'
                raise ConfigError(msg)
            if self.config.prefeeding_action == DEL_ALL_COL:
                self.feeder.delete_all_collections()
            elif self.config.prefeeding_action == DEL_COL:
                self.feeder.delete_collection_if_exists()
            elif self.config.prefeeding_action == RECREATE_COL:
                self.feeder.delete_collection_if_exists()
                self.feeder.create_collection_if_not_exists()
            elif self.config.prefeeding_action == CREATE_MISSING_COL:
                self.feeder.create_collection_if_not_exists()
            if self.feeder.collection_exists():
                if self.config.schema:
                    self.schema_manager.add_field_if_not_there()
                self.feeder.feed_documents_commit_and_process()
            else:
                raise ConfigError('Cannot feed to non-existing collection')
        if not self.feeder.collection_exists():
            raise ConfigError(f'There is no collection "{self.config.col_name}"')
        """
        if self.config.processing:
            self.processor.create_processing_job_and_wait()    
        """
        if self.config.clustering:
            self.clusterer.create_clustering_job_and_wait()
        if self.config.classification:
            self.classifier.do_classification()
        if self.config.search:
            try:
                result = self.querier.search_collection()
            except FileNotFoundError as e:
                print(f'ERROR: {str(e)}')
                return

            result_testing_type = type(self.config.search_result)
            if result_testing_type is bool or result_testing_type is int:
                testExecutor = TestExecutor(self.config)
                testExecutor.process_test_result(result)
                
                """
                if not self.config.config_source:
                    self.config.config_source = "The test"
                if type(self.config.search_result) is bool:
                    if self.config.search_result:
                        print(f'CORRECT: {self.config.config_source} is hardcoded to be correct')
                    else:
                        print(f'FAILURE: {self.config.config_source} is hardcoded to be a failure')
                elif type(self.config.search_result) is int:
                    expected_results = int(self.config.search_result)
                    actual_results = int(result['meta']['totalDocuments'])
                    if actual_results == expected_results:
                        print(f'CORRECT: {self.config.config_source} returns {actual_results} results')
                    else:
                        print(f'FAILURE: {self.config.config_source} should return {expected_results} results, not {actual_results}')
                """
     
            elif self.config.search_result == 'return':
                return result
            elif self.config.search_result == 'display':
                pretty_print(result)
            elif self.config.search_result.startswith('save:'):
                with open(self.config.search_result[len('save:'):], 'wb') as f:
                    f.write(dumps(result, indent=4).encode('utf-8'))
            else:    
                msg = '"search_result" must be "display", "return", "save:<file path>" or int'
                raise ValueError(msg)  
                
         
def run_cmd_line(cmd_line_args):
    script_path = cmd_line_args[0]
    script_name_with_ext = basename(script_path)
    script_name = basename(script_name_with_ext).split('.')[0]    
    if not exists(LOG_DIR):
        makedirs(LOG_DIR)
    log_file = join(LOG_DIR, script_name + ".log")
    logging.basicConfig(
            format = '%(asctime)s %(levelname)s: ' +
                     '%(message)s, %(filename)s(%(lineno)d)',
            datefmt = '%m/%d/%Y %H:%M:%S',
            filename = log_file,
            level = logging.DEBUG)
    
    if len(cmd_line_args) != 2:
        print(f'\nVersion: {script_name} {VERSION}')
        print(f'Usage:   python {script_name_with_ext} <config-file-path>')
    else:
        config_file_path = cmd_line_args[1]
        with open(config_file_path, 'rb') as f:
            try:
                config = loads(f.read().decode('utf-8'))
                if 'config_source' not in config:
                    config['config_source'] = config_file_path
                if 'silent_mode' not in config:
                    if run_from_cmd_line:
                        config['silent_mode'] = False
                    else:
                        config['silent_mode'] = True
            except JSONDecodeError as e:
                raise ConfigError(f'Bad JSON in config file "{config_file_path}": {e}')

            admin = Admin(Config(config))
            return admin.run_config()


run_from_cmd_line = False
if __name__ == '__main__':
    run_from_cmd_line = True
    run_cmd_line(argv)

