from json import loads, dumps
from json.decoder import JSONDecodeError
from time import sleep, time
from csv import DictReader, Sniffer
from os.path import join, basename, dirname, exists, isdir, isfile, splitext, split as path_split, getsize, isabs
from os import listdir, makedirs, walk, system, stat, getcwd, chdir
from zipfile import is_zipfile, ZipFile
from gzip import open as gzip_open
from tarfile import is_tarfile, open as tarfile_open 
from bz2 import decompress
from codecs import BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE
from shutil import copy, copytree as copy_dir_tree, rmtree as delete_dir_tree
from typing import Union, List, Optional, Tuple, Set, Dict, DefaultDict, Any
from re import search, match, sub
from random import random, shuffle
from subprocess import call
import logging
import requests
import sys

try:
    import PyPDF2
    pdf_is_suported = True
except:
    pdf_is_suported = False

log = logging.getLogger(__name__)

ROOT_SUB_DIR       = 'ayfieExpress_output'
UNZIP_DIR          = join(ROOT_SUB_DIR, 'unzipDir')
DATA_DIR           = join(ROOT_SUB_DIR, 'dataDir')
LOG_DIR            = join(ROOT_SUB_DIR, 'log')
INSPECTOR_LOG_DIR  = join(ROOT_SUB_DIR, 'inspector_download')
TMP_LOG_UNPACK_DIR = join(ROOT_SUB_DIR, 'temp_log_unpacking')

OFF_LINE           = "off-line"
OVERRIDE_CONFIG    = "override_config"
RESPECT_CONFIG     = "respect_config"

HTTP_VERBS         = ['POST', 'GET', 'PUT', 'PATCH', 'DELETE']
JOB_TYPES          = ['PROCESSING', 'CLUSTERING', 'SAMPLING', 'CLASSIFICATION']
JOB_STATES         = ['RUNNING', 'SUCCEEDED']
FIELD_TYPES        = ['TEXT_EN', 'KEYWORD', 'ENTITY', 'EMAIL_ADDRESS', 'INTEGER',
                      'DOUBLE', 'BOOLEAN', 'DATE', 'PATH']
FIELD_ROLES        = ['EMAIL_SUBJECT', 'EMAIL_BODY', 'EMAIL_SENT_DATE',
                      'EMAIL_SENDER', 'EMAIL_TO', 'EMAIL_CC', 'EMAIL_BCC',
                      'EMAIL_ATTACHMENT', 'EMAIL_CONVERSATION_INDEX',
                      'EMAIL_HEADEREXTRACTION', 'DEDUPLICATION', 'CLUSTERING',
                      'HIGHLIGHTING', 'FAST_HIGHLIGHTING']
COL_TRANS          = ['DELETE', 'RECOVER', 'COMMIT', 'PROCESS']
COL_STATES         = ['EMPTY', 'IMPORTING', 'COMMITTING', 'COMMITTED', 'PROCESSING',
                      'PROCESSED', 'ABORTED', 'DELETING']
CLA_TRANS          = ['TRAIN']
CLA_STATES         = ['INITIALIZED', 'TRAINING', 'TRAINED', 'INVALID']

DOCUMENT_TYPES     = ['email', 'default', 'autodetect']
PREPROCESS_TYPES   = ['rfc822', 'NUIX', 'IPRO']

_COL_APPEAR        = 'appear'
_COL_DISAPPEAR     = 'disappear'
_COL_EVENTS        = [_COL_APPEAR, _COL_DISAPPEAR]
_ITEM_TYPES        = ['collections', 'classifiers', 'jobs:jobinstances']

MAX_BATCH_SIZE     = 1000

DEL_ALL_COL        = "delete_all_collections"
DEL_COL            = "delete_collection"
CREATE_MISSING_COL = "create_collection_if_not_exists"
RECREATE_COL       = "recreate_collection"
NO_ACTION          = "no_action"
PRE_ACTIONS        = [NO_ACTION, DEL_ALL_COL, DEL_COL, RECREATE_COL, CREATE_MISSING_COL]

AUTO               = "auto"
AYFIE              = "ayfie"
AYFIE_RESULT       = "ayfie_result"
CSV                = "csv"
JSON               = "json"
PDF                = "pdf"
WORD_DOC           = "docx"
XLSX               = "xlsx"
TXT                = "txt"
XML                = 'xml'
DATA_TYPES         = [AYFIE, AYFIE_RESULT, JSON, CSV, PDF, TXT]
JSON_TYPES         = [AYFIE, AYFIE_RESULT, JSON]
ZIP_FILE           = 'zip'
TAR_FILE           = 'tar'
GZ_FILE            = 'gz'
BZ2_FILE           = 'bz2'
_7Z_FILE           = '7z'
RAR_FILE           = 'rar'
ZIP_FILE_TYPES     = [ZIP_FILE, TAR_FILE, GZ_FILE, BZ2_FILE, _7Z_FILE, RAR_FILE]
TEXT_ENCODINGS     = [BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE]

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

RETURN_RESULT      = 'return'
DISPLAY_RESULT     = 'display'
ID_LIST_RESULT     = 'idList'
SAVE_RESULT        = 'save:'

PROGRESS_BAR_INTERVAL = 3

CLEAR_VALUES       = "clear"
REPLACE_VALUES     = "replace"
ADD_VALUES         = "add"
DOC_UPDATE_ACTIONS = [CLEAR_VALUES, REPLACE_VALUES, ADD_VALUES]



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
        
    def __log_attempt_and_go_to_sleep(self, tries, error_msg, go_to_sleep): 
        sleep_msg = ""
        if go_to_sleep:
            sleep_interval = tries * 5
            if sleep_interval > MAX_PAUSE_BEFORE_RETRY:
                sleep_interval = MAX_PAUSE_BEFORE_RETRY 
            sleep(sleep_interval)
            sleep_msg = ". Trying again in {sleep_interval} seconds"
        msg = f"Connection issue: {error_msg}{sleep_msg}"
        log.debug(msg)
        print(msg)
            
    def __execute(self, path, verb, data={}):
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
                self.__log_attempt_and_go_to_sleep(tries, str(e), go_to_sleep=True)
            except MemoryError:
                self.__log_attempt_and_go_to_sleep(tries, "Out of memory - giving up!", go_to_sleep=False)
                raise
            except Exception as e:
                self.__log_attempt_and_go_to_sleep(tries, "Unknown/unhandled error reason", go_to_sleep=False) 
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
        if id_from_headers and not 'batches' in path:
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

    def process_collection(self, col_id, config={}):
        self.create_job(config)

    def train_collection(self, classifier_name):
        # Need to be merge with collection transitions
        data = {"classifierState": 'TRAIN'}
        return self.__execute(f'classifiers/{classifier_name}', 'PATCH', data)

    def commit_collection_and_wait(self, col_id, timeout=300):
        self.commit_collection(col_id)
        self.__wait_for_collection_to_be_committed(col_id, timeout)

    def process_collection_and_wait(self, col_id, config={}, timeout=300):
        self.create_job_and_wait(config)

    def train_collection_and_wait(self, col_id, timeout=300):
        self.train_collection(col_id)
        self.__wait_for_collection_to_be_trained(col_id, timeout)

    def feed_collection_documents(self, col_id, documents):
        path = f'collections/{col_id}/batches'
        data = {'documents': documents}
        return self.__execute(path, 'POST', data)

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
        response = self.feed_collection_documents(col_id, documents)
        self.commit_collection_and_wait(col_id, timeout=300)
        self.process_collection_and_wait(col_id, timeout=300)
        return response

    def json_based_meta_search(self, col_id, json_query):
        path = f'collections/{col_id}/search'
        return self.__execute(path, 'POST', json_query)

    def json_based_meta_concept_search(self, col_id, json_query):
        path = f'collections/{col_id}/search/concept'
        return self.__execute(path, 'POST', json_query)

    def json_based_doc_search(self, col_id, json_query):
        return self.json_based_meta_search(col_id, json_query)['result']

    def search_collection(self, col_id, query, size=10, offset=0,
                          filters=[], exclude=[], scroll=False, meta_data=False):
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
            "scroll": scroll,
            "size": size,
            "offset": 0
        }
        if meta_data:
            return self.json_based_meta_search(col_id, json_query)
        else:
            return self.json_based_doc_search(col_id, json_query)
        
    def next_scroll_page(self, link):
        return self.__execute("/".join(link.replace('//', "").split('/')[3:]), 'GET')
 
    ######### Jobs #########

    def create_job(self, job_config):
        return self.__execute('jobs', 'POST', job_config)

    def create_job_and_wait(self, job_config):
        job_id = self.create_job(job_config)
        jobsStatus = JobsStatus(self.server, self.port, self.version)
        return jobsStatus.wait_for_job_to_finish(job_id)

    def get_jobs(self):
        return self.__execute('jobs', 'GET')
        
    def get_job(self, job_id, verbose=None):
        endpoint = f'jobs/{job_id}'
        if verbose:
            endpoint += "?verbose=true"
        return self.__execute(endpoint, 'GET')

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
        return self.create_job_and_wait(config)
        

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

    def update_document(self, col_id, doc_id, field, values):
        data = {field: values}
        endpoint = f'collections/{col_id}/documents/{doc_id}'
        self.__execute(endpoint, 'PATCH', data)

    def update_documents(self, col_id, doc_ids, field, values):
        for doc_id in doc_ids:
            self.update_document(col_id, doc_id, field, values)  
            
    def update_documents_by_query(self, col_id, query, filters, update_action, field, values=None):
        if not update_action in DOC_UPDATE_ACTIONS:
            raise ValueError(f"Parameter update_action cannot be {update_action}, but must be in {DOC_UPDATE_ACTIONS}")
        data = {field : values}
        if update_action == CLEAR_VALUES:
            data = [field]
        data = {"query" : {"query" : query}, update_action : data}
        if filters:
            data['query']['filters'] = filters  
        endpoint = f'collections/{col_id}/documents'
        return self.__execute(endpoint, 'PATCH', data)
        
    def update_documents_by_query_and_wait(self, col_id, query, filters, action, field, values):
        job_id = self.update_documents_by_query(col_id, query, filters, action, field, values)
        jobsStatus = JobsStatus(self.server, self.port, self.version)
        return jobsStatus.wait_for_job_to_finish(job_id)
 
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
      
        elapsed_time = self.create_job_and_wait(job_config)
        
        
class ExpressTools():

    def __init__(self, ayfie):
        self.ayfie = ayfie
 
    def get_all_doc_ids(self, col_id, limit=None, random_order=False):
        if not limit:
            limit = "ALL"
        docs_per_request = 10000
        if limit and limit != "ALL" and limit < docs_per_request:
            docs_per_request = limit
        result = self.ayfie.search_collection(col_id, "*", docs_per_request, 
                 exclude=["content", "term", "location", "organization", "person", "email"], 
                 scroll=True, meta_data=True)
        all_ids = [doc['document']['id'] for doc in result["result"]]
        while True:
            try:
                link = result["_links"]["next"]["href"]
            except:
                break
            result = self.ayfie.next_scroll_page(link)
            ids = [doc['document']['id'] for doc in result["result"]]
            if len(ids) == 0:
                break
            all_ids += ids
            if limit != "ALL" and len(all_ids) >= int(limit):
                break
        if limit != "ALL" and limit < len(all_ids):
            all_ids = all_ids[:limit]
        if random_order:
            shuffle(all_ids)
        return all_ids
        
        
class WebContent():
        
    def download(self, url, directory):
        response = requests.get(url, allow_redirects=True)
        filename = self.__get_filename_from_response(response)
        if not filename:
            filename = url.split('/')[-1]
        path = join(directory, filename)
        with open(path, 'wb') as f:
            f.write(response.content)
        return path
                
    def __get_filename_from_response(self, response):
        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filenames = re.findall('filename=(.+)', content_disposition)
            if len(filenames) > 0:
                return filenames[0]
        return None


class FileHandlingTools():
 
    def get_file_type(self, file_path):
        detected_encoding = None
        if isfile(file_path):
            extension = splitext(file_path)[-1].lower()
            if is_zipfile(file_path):
                if extension == ".docx":
                    return WORD_DOC, detected_encoding
                elif extension == ".xlsx":
                    return XLSX, detected_encoding
                return ZIP_FILE, detected_encoding
            elif is_tarfile(file_path):
                return TAR_FILE, detected_encoding
            else:
                with open(file_path, 'rb') as f:
                    start_byte_sequence = f.read(100)
                for pattern in FILE_SIGNATURES.keys():
                    if start_byte_sequence.startswith(pattern):
                        if pattern in TEXT_ENCODINGS:
                            self.detected_encoding = BOM_MARKS_NAMES[pattern]
                            log.debug(f"'{file_path}' encoding auto detected to '{self.detected_encoding}'")
                            start_byte_sequence = start_byte_sequence[len(pattern):]
                            return self.__get_text_format_type(start_byte_sequence, extension), detected_encoding
                        return FILE_SIGNATURES[pattern], detected_encoding
                return self.__get_text_format_type(start_byte_sequence, extension), detected_encoding
        return None, detected_encoding
        
    def __get_text_format_type(self, start_byte_sequence, extension):
        start_char_sequence = start_byte_sequence.decode('iso-8859-1')
        org_start_char_sequence = start_char_sequence
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
            try:
                dialect = Sniffer().sniff(org_start_char_sequence)
                return CSV
            except:
                return None
        return None

    def unzip(self, file_type, file_path, unzip_dir):
        if file_type not in ZIP_FILE_TYPES:
            return False
        if not unzip_dir:
            raise ValueError("No unzip directory is given")
        self.recreate_directory(unzip_dir)
        if file_type == ZIP_FILE:
            with ZipFile(file_path, 'r') as z:
                z.extractall(unzip_dir)
        elif file_type == TAR_FILE:
            with tarfile_open(file_path) as tar:
                tar.extractall(unzip_dir)
        elif file_type == GZ_FILE:
            with open(unzip_dir, 'wb') as f, gzip_open(file_path, 'rb') as z:
                f.write(z.read())
        elif file_type == BZ2_FILE:
            with open(unzip_dir, 'wb') as f, open(file_path,'rb') as z:
                f.write(decompress(z.read()))
        elif file_type == _7Z_FILE:
            log.info(f"Skipping '{file_path}' - 7z decompression still not implemented")
        elif file_type == RAR_FILE:
            log.info(f"Skipping '{file_path}' - rar decompression still not implemented")
        else:
            raise ValueError(f'Unknown zip file type "{zip_file_type}"')
        return True
        
        
    def delete_directory(self, dir_path): 
        tries = 0
        while exists(dir_path):
            if tries > 5:
                raise IOError(f"Failed to delete directory '{dir_path}'")    
            delete_dir_tree(dir_path)
            tries += 1
            sleep(5 * tries)

    def recreate_directory(self, dir_path):
        self.delete_directory(dir_path)
        makedirs(dir_path)
        if not exists(dir_path):
            raise IOError("Failed to create directory '{dir_path}'") 

    def get_next_file(self, root, dir_filter=None, extension_filter=None):
        items = listdir(root)
        files = [item for item in items if isfile(join(root, item))]
        dirs = [item for item in items if isdir(join(root, item))]
        for f in files:
            path_without_extension, extension = splitext(f)
            if extension_filter:
                if extension in extension_filter:
                    yield join(root, f)
            else:
                yield join(root, f)
        for dir in dirs:
            if dir_filter:
                if dir in dir_filter:
                    continue
            for f in self.get_next_file(join(root, dir), dir_filter, extension_filter):
                yield f

    def __write_fragment_to_file(self, dir_name, fragment_name, fragment_count, fragment_lines):
        with open(join(dir_name, f"{fragment_name}_{fragment_count}"), "wb") as f:
            f.write(''.join(fragment_lines).encode('utf-8')) 
            
    def split_files(self, dir_path, lines_per_fragment=0, max_fragments=0):
        for log_file in self.get_next_file(dir_path):
            self.split_file(log_file, lines_per_fragment, max_fragments)            
                
    def split_file(self, file_path, lines_per_fragment=0, max_fragments=0):
        lines_per_fragment = int(lines_per_fragment)
        max_fragments = int(max_fragments)
        line_count = 0
        fragment_count = 0
        fragment_name = basename(file_path)
        dir_name = dirname(file_path)
        fragment_lines = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                fragment_lines.append(line)
                if lines_per_fragment and line_count >= lines_per_fragment - 1:
                    self.__write_fragment_to_file(dir_name, fragment_name, fragment_count, fragment_lines)                    
                    line_count = 0
                    fragment_lines = []
                    fragment_count += 1
                    if max_fragments and fragment_count >= max_fragments:
                        return
                else:
                    line_count += 1
        if line_count > 0: 
            self.__write_fragment_to_file(dir_name, fragment_name, fragment_count, fragment_lines)
         

class DataSource():

    def __init__(self, config, unzip_dir=UNZIP_DIR):
        self.config = config
        self.created_temp_data_dir = False
        if self.config.data_source == None:
            raise ValueError(f"The new parameter 'data_source' is mandatory and used instead of both 'data_dir' and 'data_file'. Replace any of these with 'data_source' if you are still using those in your configuration.")
        if not exists(self.config.data_source):
            raise ValueError(f"There is no file or directory called '{self.config.data_source}'")
        if isdir(self.config.data_source):
            self.data_dir = self.config.data_source
        elif isfile(self.config.data_source):
            self.data_dir = DATA_DIR + '_' +  str(int(random() * 10000000))
            self.created_temp_data_dir = True
            FileHandlingTools().recreate_directory(self.data_dir)
            copy(self.config.data_source, self.data_dir)
        else:
            msg = f'Source path "{self.config.data_source}" is neither a directory, '
            msg += 'a regular file nor a supported zip file type.'
            raise ValueError(msg)
        self.unzip_dir = unzip_dir
        self.total_retrieved_file_count = 0
        self.total_retrieved_file_size = 0

    def __del__(self):
        if self.created_temp_data_dir:
            delete_dir_tree(self.data_dir)
            
    def __get_file_type(self, file_path):
        return FileHandlingTools().get_file_type(file_path)

    def __unzip(self, file_type, file_path, unzip_dir):
        return FileHandlingTools().unzip(file_type, file_path, unzip_dir)

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
        doc_size = 0
        if self.config.report_fed_doc_size or self.config.max_doc_characters or self.config.min_doc_characters:
            doc_size = len(content)
            if self.config.second_content_field:
                doc_size += doc_size
        if self.config.second_content_field:
            doc["fields"][self.config.second_content_field] = content
        if self.config.report_fed_doc_size:
            self._print(f"Doc size for document with id {id} is {doc_size} characters") 
        if ((self.config.max_doc_characters and (doc_size > self.config.max_doc_characters)) 
                     or (self.config.min_doc_characters and (doc_size < self.config.min_doc_characters))):   
            log.info(f"Document with id {id} dropped due to a size of {doc_size} characters")    
            return None
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

    def __get_document(self, file_path, auto_detected_file_type, is_training_doc=False):
        retrieve_document = True
        if self.config.file_picking_list:
            retrieve_document = False
            filename = basename(file_path)
            if filename in self.config.file_picking_list or f"{splitext(filename)[0]}" in self.config.file_picking_list:
                retrieve_document = True                 
        if not retrieve_document:
            return None
            
        data_type = self.config.data_type
        if data_type == AUTO:
            data_type = auto_detected_file_type
            if not data_type:
                log.error(f'File type detetection failed for "{file_path}"')
                return None
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
                                yield self.__construct_doc(row[mappings['id']], training_value)
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
                            yield self.__construct_doc(document["document"]["id"], document["document"]["content"])
                        else:
                            yield document
                except JSONDecodeError as e:
                    log.error(f"JSON decoding error for {file_path}: {str(e)}")
                    return None
            elif data_type == PDF:
                yield self.__construct_doc(self.__gen_id_from_file_path(file_path),
                                             self.__convert_pdf_to_text(file_path))
            elif data_type in [WORD_DOC, XLSX, XML]:
                log.info(f"Skipping '{file_path}' - conversion of {data_type} still not implemented")
            elif data_type == TXT:
                yield self.__construct_doc(self.__gen_id_from_file_path(file_path), self.__get_file_content(file_path))
            else:
                log.error(f"Unknown data type '{data_type}'")

    def get_documents(self, is_training_doc=False):
        self.file_size = {}
        self.file_paths = []
        for file_path in FileHandlingTools().get_next_file(self.data_dir, extension_filter=self.config.file_extension_filter):
            file_type, self.detected_encoding = self.__get_file_type(file_path)
            if self.config.file_type_filter and not file_type in self.config.file_type_filter:
                log.debug(f"'{file_path}' auto detected (1) as '{file_type}' and not excluded due to file type filter: {self.config.file_type_filter}")
                continue
            log.debug(f"'{file_path}' auto detected (1) as '{file_type}'")
            if file_type in ZIP_FILE_TYPES:
                self.__unzip(file_type, file_path, self.unzip_dir)
                for unzipped_file in FileHandlingTools().get_next_file(self.unzip_dir):
                    file_type, self.detected_encoding = self.__get_file_type(unzipped_file)
                    if self.config.file_type_filter and not file_type in self.config.file_type_filter:
                        log.debug(f"'{file_path}' auto detected (2) as '{file_type} and not excluded due to file type filter: {self.config.file_type_filter}")
                        continue
                    if file_type in ZIP_FILE_TYPES:
                        log.info(f"Skipping '{unzipped_file}' - zip files within zip files are not supported")
                        continue
                    log.debug(f"'{file_path}' auto detected (2) as '{file_type}'")
                    for document in self.__get_document(unzipped_file, file_type, is_training_doc):
                        if document:
                            self.updateFileStatistics(unzipped_file)
                            yield document
            else:
                for document in self.__get_document(file_path, file_type, is_training_doc):
                    if document:
                        self.file_paths.append(file_path)
                        self.updateFileStatistics(file_path)
                        yield document
                        
    def pop_file_paths(self):
        for path in self.file_paths:
            yield path
        self.file_paths = []
        
    def updateFileStatistics(self, file_path):
        if not file_path in self.file_size:
            self.file_size[file_path] = stat(file_path).st_size
        
    def get_file_statistics(self):
        total_size = 0
        for file_path in self.file_size:
            total_size += self.file_size[file_path]
        return len(self.file_size), total_size


class AyfieConnector():
    
    def __init__(self, config, data_source=None, additional_data_source=None):
        self.data_source = data_source
        self.additional_data_source = additional_data_source
        self.config = config
        args = [self.config.server, self.config.port, self.config.api_version]
        self.ayfie = Ayfie(*args)
        
    def _print(self, message, end='\n'):
        if not self.config.silent_mode:
            print(message, end=end)
        
        
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
            self._print([doc['id'] for doc in self.batch])
        if self.config.ayfie_json_dump_file:
            with open(self.config.ayfie_json_dump_file, "wb") as f:
                try:
                    f.write(dumps({'documents': self.batch}, indent=4).encode('utf-8'))
                except Exception as e:
                    log.warning(f'Writing copy of batch to be sent to disk failed with the message: {str(e)}')    
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        log.info(f'About to feed batch of {len(self.batch)} documents')
        return self.ayfie.feed_collection_documents(col_id, self.batch)

    def process_batch(self, is_last_batch=False):
        if self.config.file_copy_destination: 
            if not exists(self.config.file_copy_destination):
                makedirs(self.config.file_copy_destination) 
            for from_file_path in self.data_source.pop_file_paths():
                if self.config.file_copy_destination:
                    copy(from_file_path, self.config.file_copy_destination)
                elif False:  # Salted version of the line above
                    dummy, filename = path_split(from_file_path)
                    to_file_path = join(self.config.file_copy_destination, str(int(random() * 10000000)) + '_' + filename)
                    copy(from_file_path, to_file_path) 
        self.files_to_copy = []
        if self.config.feeding_disabled:
            self.doc_count += self.batch_size
            self._print(f"Feeding disabled, {self.doc_count} documents not uploaded to collection '{self.config.col_name}'")
        else:
            batch_failed = False
            try:
                response = self.__send_batch()
                if self.config.report_doc_error:
                    self._print(dumps(response, indent=4))
            except MemoryError:
                batch_failed = True
                err_message = "Out of memory"
            except Exception as e:
                batch_failed = True
                err_message = str(e)
            if batch_failed:
                sentence_start = "A"
                if is_last_batch:
                    sentence_start = "A last"
                err_message = f"{sentence_start} batch of {self.batch_size} docs fed to collection '{self.config.col_name}' failed: '{err_message}'"
                self._print(err_message)
                log.error(err_message)
            else:
                self.doc_count += self.batch_size
                sentence_start = "So far"
                if is_last_batch:
                    sentence_start = "A total of" 
                self._print(f"{sentence_start} {self.doc_count} documents has been uploaded to collection '{self.config.col_name}'")
        self.batch = []
        self.batch_size = 0
        
    def feed_documents(self):
        self.batch = []
        self.doc_count = 0
        self.batch_size = 0
        self.files_to_copy = []
        if not self.data_source:
            log.warning('No data source assigned')
            return
        failure = None
        log_message = ""
        start_time = time()
        id_picking_list = [str(id) for id in self.config.id_picking_list]
        try:
            for document in self.data_source.get_documents():
                if id_picking_list:
                    if not (str(document["id"]) in id_picking_list):
                        continue
                self.batch.append(document)
                self.batch_size = len(self.batch)
                if self.batch_size >= self.config.batch_size:
                    self.process_batch()
            if self.batch_size:
                self.process_batch(is_last_batch=True)
            else:
                if self.config.feeding_disabled:
                    self._print(f"Feeding has been turned off")
                    self.total_retrieved_file_count = 0
        except Exception as e:
            log_message = " before script crashed"
            raise
        finally:
            elapsed_time = int(time() - start_time)
            file_count, total_file_size = self.data_source.get_file_statistics()
            log_message = f"A total of {file_count} files and {total_file_size} bytes were retrieved from disk" + log_message
            log.info(log_message)
            self._print(log_message)
        return elapsed_time
                
    def job_reporting_output_line(self, job, prefix, end="\n"):
        self._print(f'{prefix}: {job["state"].ljust(10)} {job["id"].ljust(10)} {job["type"].ljust(22)} {job["clock_time"].ljust(8)}', end)
             
    def process(self):
        elapsed_time = -1
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        job_config = {
            "collectionId" : col_id,
            "type" : "PROCESSING"
        }
        if self.config.processing:
            job_config["settings"] = self.config.processing
        if self.config.progress_bar:
            job_id = self.ayfie.create_job(job_config)
            jobsStatus = JobsStatus(self.config.server, self.config.port, self.config.api_version)
            completed_sub_job_ids = []
            start_time = None
            while True:
                all_job_statuses = jobsStatus.get_job_and_sub_job_status_obj(job_id, completed_sub_job_ids)
                if start_time == None and all_job_statuses["job"]['state'] in ['RUNNING','SUCCEEDED','FAILURE']:
                    start_time = time()
                elapsed_time = int(time() - start_time)
                for sub_job in all_job_statuses["sub_jobs"]:
                    end = "\r"
                    if not sub_job["state"] in ["SCHEDULED", "RUNNING"]:
                        completed_sub_job_ids += [sub_job["id"] for sub_job in all_job_statuses["sub_jobs"]]
                        end = "\n"
                    if not sub_job["state"] in ["SCHEDULED"]:
                        self.job_reporting_output_line(sub_job, 'sub-job', end)
                job = all_job_statuses["job"]
                if job['state'] in ["SUCCEEDED", "FAILURE"]:
                    self.job_reporting_output_line(job, '    JOB', "\n")
                    break
                sleep(PROGRESS_BAR_INTERVAL)
        else:
            elapsed_time = self.ayfie.create_job_and_wait(job_config)
        return elapsed_time
        
    def feed_documents_and_commit(self):
        feeding_time = self.feed_documents()
        if self.config.feeding_report_time:
            self._print(f"The feeding operation took {str(feeding_time)} seconds")
        if self.config.feeding_disabled:
            log.info(f'No commit or processing as feeding is disabled')
            return
        if self.doc_count <= 0:
            log.info(f'No documents to feed')
            return
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        log.info(f'Fed {self.doc_count} documents. Starting to commit')
        self.ayfie.commit_collection_and_wait(col_id)
        log.info(f'Done committing')

    def feed_documents_commit_and_process(self):
        self.feed_documents_and_commit()
        log.info(f'Starting to process')
        processing_time = self.process()
        if self.config.feeding_report_time:
            self._print(f"The processing operation took {str(processing_time)} seconds")
        log.info(f'Done processing')


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
            self.ayfie.update_documents(self.col_id,
                                     self.config.training_set,
                                     self.config.training_field,
                                     ['tagged'])
        else:
            batch = []
            for document in self.training_source.get_documents(True):
                self.ayfie.update_document(self.col_id,
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
                "filters" : self.config.clustering_filters,
                "outputField" : self.config.clustering_output_field
            }
            log.info('Starting clustering job')  # TEMP Solution
            elapsed_time = self.ayfie.create_job_and_wait(cluster_config)
            if self.config.clustering_report_time:
                self._print(f"The clustering operation took {str(elapsed_time)} seconds")
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
        
class Reporter(AyfieConnector):

    def __get_jobs_status(self):     
        jobsStatus = JobsStatus(self.config.server, self.config.port)
        col_id = None
        job_id = None
        latest_job_only = False
        if self.config.report_jobs in ["collection", "collection_latest"]:
            col_id = self.ayfie.get_collection_id(self.config.col_name)
            if self.config.report_jobs == "collection_latest":
                latest_job_only = True
        elif type(self.config.report_jobs) is int:
            job_id = self.config.report_jobs
        elif self.config.report_jobs != "all":
            raise ValueError(f"Unknown jobs value '{self.config.report_jobs}'")
        main_job_only = True
        jobsStatus.print_jobs_overview(None, col_id, job_id, latest_job_only, self.config.report_verbose)
                 
    def __get_logs_status(self):
        if self.config.report_logs_source and self.config.report_retrieve_logs:
            raise ValueError("Reporting options 'logs_source' and 'retrieve_logs' cannot both be set")
        if self.config.report_logs_source:
            if self.config.report_logs_source.startswith('http'):
                FileHandlingTools().recreate_directory(INSPECTOR_LOG_DIR)
                log_file_path = WebContent().download(self.config.report_logs_source, INSPECTOR_LOG_DIR)
            else:
                log_file_path = self.config.report_logs_source
        elif self.config.report_retrieve_logs:
            old_path = getcwd()
            chdir(self.config.report_retrieve_logs)
            log_file = "inspector-logs.txt"
            log_file_path = join(self.config.report_retrieve_logs, log_file)
            try:
                with open(log_file, "a") as f:
                    call(f"docker-compose logs -t > {log_file}", shell=True, stdout=f, stderr=f)
            except:
                raise
            finally:
                chdir(old_path)
        else:
            return

        if not exists(log_file_path):
            raise ValueError(f"'{log_file_path}' does not exist")
        LogAnalyzer(log_file_path, self.config.report_output_destination, TMP_LOG_UNPACK_DIR).analyze()

        
    def __get_memory_config(self):
        print ("got here")

    def do_reporting(self):
        if self.config.report_jobs:
            self.__get_jobs_status()
        if self.config.report_logs_source or self.config.report_retrieve_logs:
            self.__get_logs_status()
      
class DocumentUpdater(AyfieConnector):

    def __update_documents_by_query(self, query, filters, update_action, field, values=None):
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        return self.ayfie.update_documents_by_query_and_wait(col_id, query, filters, update_action, field, values)
            
    def __get_all_doc_ids(self, limit=None, random_order=False):
        col_id = self.ayfie.get_collection_id(self.config.col_name)
        return ayfieTools.get_all_doc_ids(col_id, limit, random_order)
        
    def do_updates(self):
        filters = []
        if self.config.doc_update_filters:
            filters = self.config.doc_update_filters
            if self.config.doc_update_filters == "DOC_IDS":
                filters = [{"field":"_id", "value": self.__get_all_doc_ids(self.config.numb_of_docs_to_update)}]
        elapsed_time = self.__update_documents_by_query(self.config.doc_update_query, filters, 
                                       self.config.doc_update_action, self.config.doc_update_field, 
                                       self.config.doc_update_values)
        if self.config.doc_update_report_time:
            self._print(f"The documents update operation took {str(elapsed_time)} seconds")
                       
class LogAnalyzer():

    def __init__(self, log_file_path, output_destination, log_unpacking_dir):
        self.output_destination = output_destination
        self.log_file = log_file_path
        self.log_unpacking_dir = log_unpacking_dir
        self.unzip_dir = UNZIP_DIR
        if not exists(log_file_path):
            raise ConfigError(f"There is no {log_file_path}")
        self.info = [
            {
                "pattern" : r"^.*m[a-z]*_[0-9]*\s.*Container is mem limited to .*G \(of (.*G) of the host\).*$",
                "extraction": [("Total RAM on host", 1)]
            },
            {
                "pattern" : r"^.*m([a-z]*_[0-9]*)\s.*Container is mem limited to (.*[GM][B]?) \(of .*[GM][B]? of the host\).*$",
                "extraction": [("Container", 1), ("Assigned RAM", 2)]
            },
            {
                "pattern" : r"^.*assigning (.* MB) to spark, driver receives (.*M), worker and executor receive (.*M).*$",
                "extraction": [("Spark memory", 1), ("Driver memory", 2), ("Worker & Executor memory", 3)]
            },
            {
                "pattern" : r"^.*export ([A-Z]+(?:_[A-Z]+)*)=([0-9]+[kKMGT]?)$",
                "extraction": [(1, 2)]
            }
        ]
        self.symptoms = [
            {
                "pattern" : r"^.*Ignoring unknown properties with keys.*$",
                "indication": "one has used an API parameter that does not exist or is mispelled."
            },
            {
                "pattern" : r"^.*All masters are unresponsive.*$",
                "indication": "there was a unsufficient process clean up after a failure. Corrective action would be to bounce the api container."
            },
            {
                "pattern" : r"^.*Missing an output location for shuffle.*$",
                "indication": "the system has run out of memory"
            },
            {
                "pattern" : r"^.*None of the configured nodes are available.*$",
                "indication": "the host is low on disk and/or memory."
            },
            {
                "pattern" : r"^.*java\.lang\.OutOfMemoryError.*$",
                "indication": "the host has run out of memory."
            },            
            {
                "pattern" : r"^.*all nodes failed.*$",
                "indication": "the disk is too slow for the given load."
            },
            {
                "pattern" : r"^.*no other nodes left.*$",
                "indication": "the disk is too slow for the given load."
            },
            {
                "pattern" : r"^.*all shards failed.*$",
                "indication": "something is off with the Elasticsearch index partitions."
            },
            {
                "pattern" : r"^.*index read-only / allow delete \(api\).*$",
                "indication": "The system is low on disk and Elasticsearch has write protecting the index to prevent possible index corruption."
            },            
            {
                "pattern" : r"^.*Timeout while waiting for ltlocate.*$",
                "indication": "an entity extraction job took too long and timed out."
            },
            {
                "pattern" : r"^.*ltlocate extraction result parsing failed.*$",
                "indication": "the json ltlocate extraction result parsing timed out, or somewhat less likely, it contained incorrect escape characters."
            },
            {
                "pattern" : r"^.*IndexNotFoundException\[no such index\].*$",
                "indication": 'the referenced collection was only partly deleted (meta data doc still exists). Do complete deletion with: docker exec -it <elasticsearch container name> curl -XDELETE -H Content-Type:application/json 127.0.0.1:9200/.ayfie-collection-metadata-v1/collection/<collection_id>'
            }, 
            {
                "pattern" : r"^.*Ignoring unknown properties with keys.*$",
                "indication": "a possible wrong API parameter has been applied with the likely result that the intended operation never got carried out."
            },
            {
                "pattern" : r"^.*Could not validate license. License details.*$",
                "indication": "the Chinese tokenizer has an expired license"
            },            
            {
                "pattern" : r"^.*INFO: Exception.*at (org|scala|java)\..*\(.*:[0-9]+\).*.\)      at java.lang.Thread.run\(Thread.java:748\)$",
                "indication": "this is just a test for exceptions in general"
            }
        ] 
        
    def clear_counters(self):
        for symptom in self.symptoms:
            symptom["occurences"] = 0
            symptom["last_occurence_line"] = None
            symptom["last_occurence_sample"] = None
        self.errors_detected = False
        self.exceptions = {}
        
    def __prepare_temp_log_directory(self, log_file, log_dir):
        fht = FileHandlingTools()
        file_type, detected_encoding = fht.get_file_type(log_file)
        if fht.unzip(file_type, log_file, self.unzip_dir):
            fht.delete_directory(log_dir)
            copy_dir_tree(self.unzip_dir, log_dir)
        else:
            fht.recreate_directory(log_dir)
            copy(log_file, log_dir)
        
    def __process_log_file(self, log_file):
        if not exists(log_file):
            raise ValueError(f"Path '{log_file}' does not exists")
        if isdir(log_file):
            raise ValueError(f"'{log_file}' is not a file, but a directory")
        with open(log_file, "r", encoding="utf-8") as f:
            self.line_count = 0
            self.info_pieces = {}
            try:
                for line in f:
                    if self.line_count == 0:
                        if not line.startswith("Attaching to"):
                            raise DataFormatError(f"File '{log_file}' is not recognized as a ayfie Inspector log file")
                    self.line_count += 1
                    for info in self.info:
                        m = match(info["pattern"], line)
                        if m:
                            output_line = []
                            for item in info["extraction"]:
                                item_name = item[0]
                                if type(item_name) is int:
                                    item_name = m.group(item[0])
                                output_line.append(f"{item_name}: {m.group(item[1])}")
                            self.info_pieces[", ".join(output_line)] = True
                    for symptom in self.symptoms:
                        m = match(symptom["pattern"], line)
                        if m:
                            self.errors_detected = True
                            symptom["occurences"] += 1
                            symptom["last_occurence_line"] = self.line_count
                            symptom["last_occurence_sample"] = m.group(0)
                    m = match(r"Exception", line)
                    if m:
                        if not m.group(0) in self.exceptions:
                            self.exceptions[m.group(0)] = 0
                        self.exceptions[m.group(0)] += 1
            except UnicodeDecodeError as e:
                raise DataFormatError(f"'File {log_file}' is most likley a binary file (or at a mininimum of wrong encoding)")
            except Exception:
                raise
                        
    def analyze(self):
        if isfile(self.log_file):
            self.__prepare_temp_log_directory(self.log_file, self.log_unpacking_dir)
        elif isdir(self.log_file):
            self.log_unpacking_dir = self.log_file
        else:
            ValueError(f"'{self.log_file}' is neither a file nor a directory")
        for log_file in FileHandlingTools().get_next_file(self.log_unpacking_dir):
            self.clear_counters()
            analyses_output = ""
            is_log_file = True
            line_info = ""
            try:
                self.__process_log_file(log_file)
                line_info = f'({self.line_count} LINES) '
            except DataFormatError:
                is_log_file = False
            analyses_output += f'\n\n====== FILE "{log_file}" {line_info}======\n'
            if is_log_file:
                """
                for exception in self.exceptions:
                    print(exception, self.exceptions[exception]) 
                """ 
                if len(self.info_pieces) > 0:
                    analyses_output += "\n====== System Information ======\n"
                    for info_piece in self.info_pieces.keys():
                        analyses_output += f"{info_piece}\n"
                    
                if self.errors_detected:
                    for symptom in self.symptoms:
                        if symptom["occurences"] > 0:
                            analyses_output += "\n====== Failure Indicator ======\n"
                            analyses_output += f'There are {symptom["occurences"]} occurrences (last one on line {symptom["last_occurence_line"]}) of this message:\n\n'
                            analyses_output += f'{symptom["last_occurence_sample"]}\n\n'
                            analyses_output += f'The message could possibly indicate that {symptom["indication"]}\n'
                else:
                    analyses_output += "\n  NO KNOWN ERRORS DETECTED"
            else:
                analyses_output += "\n  DOES NOT SEEM TO BE AN AYFIE INSPECTOR LOG FILE"
                
            if self.output_destination == "terminal":
                print(analyses_output)
            else:
                with open(self.output_destination, 'wb') as f:
                    f.write(analyses_output.encode('utf-8'))

                      
class JobsStatus():

    def __init__(self, server='127.0.0.1', port='80', version='v1'):
        self.ayfie = Ayfie(server, port, version)

    def __extract_job_info(self, job):
        item = {}
        if "collectionId" in job["definition"]:
            item["collectionId"] = job["definition"]["collectionId"]
        item["type"] = job["definition"]["type"]
        item["id"] = job["id"]
        item["state"] = job["state"]
        item['sub_jobs'] = []
        try:
            item['clock_time'] = job["details"]['telemetry']['TIME_TOTAL']
        except:
            item['clock_time'] = "N/A"
        item['time_stamp'] = job["details"]["timestamp"]
        if "_links" in job:
            for key in job["_links"].keys(): 
                sub_jobs = []
                if key == key.upper():
                    links = job["_links"][key]
                    if type(links) is dict:
                        sub_jobs = [links['href'].split('/')[-1]]
                    elif type(links) is list:
                        sub_jobs = [link['href'].split('/')[-1] for link in links]
                    else:
                        raise Exception("Unknown links value type")
                if key == "previous" or key == "next":
                    item[key] = job["_links"][key]['href'].split('/')[-1]
                if sub_jobs:
                    for sub_job in sub_jobs:
                        item['sub_jobs'].append(sub_job)
        return item

    def __get_ordered_sub_job_list(self, job):
        if len(job['sub_jobs']):
            sub_job_id = job['sub_jobs'][0]
            while True:
                sub_job = self.__get_job(sub_job_id)
                if "previous" in sub_job:
                    sub_job_id = sub_job["previous"]
                else:
                    break
            ordered_sub_job_list = [sub_job_id]      
            while True:
                sub_job = self.__get_job(sub_job_id)
                if "next" in sub_job:
                    sub_job_id = sub_job["next"]
                    ordered_sub_job_list.append(sub_job_id)
                else:
                    break
            return ordered_sub_job_list
        return []

    def __gen_auxilary_data_structure(self, json_job_list):
        # Creates the following lookup table and data structures for later use
        # self.job_or_sub_job_by_id
        # self.job_and_sub_jobs_structure
        
        self.job_or_sub_job_by_id = {}
        self.job_and_sub_jobs_structure = {}
        job_by_collection_id = {}
        job_list = loads(json_job_list)
        if not "_embedded" in job_list:
            return
        if not "jobinstances" in job_list["_embedded"]:
            return
        for job in job_list["_embedded"]["jobinstances"]:
            job_info = self.__extract_job_info(job)
            if job_info["type"] == "PROCESSING":
                if not "collectionId" in job_info:
                    continue
                collection_id = job_info["collectionId"]   
                if not collection_id in job_by_collection_id:
                    job_by_collection_id[collection_id] = []
                job_by_collection_id[collection_id].append(job_info)
            self.job_or_sub_job_by_id[job_info["id"]] = job_info
        for collection_id in job_by_collection_id.keys():
            job_by_collection_id[collection_id] = sorted(job_by_collection_id[collection_id], key=lambda job_info: job_info["time_stamp"])
        for collection_id in job_by_collection_id.keys():
            self.job_and_sub_jobs_structure[collection_id] = {}
            for job in job_by_collection_id[collection_id]:
                self.job_and_sub_jobs_structure[collection_id][job["id"]] = []
                for sub_job_id in job["sub_jobs"]:
                    self.job_and_sub_jobs_structure[collection_id][job["id"]].append(sub_job_id)
        for collection_id in job_by_collection_id.keys():
            for job in job_by_collection_id[collection_id]:
                job['sub_jobs'] = self.__get_ordered_sub_job_list(job)
        
    def __get_job(self, id):
        return self.job_or_sub_job_by_id[id]
           
    def __get_list_of_sub_jobs(self, json_job_list, job_id, sub_jobs_to_ignore):
        sub_jobs_list = []
        for collection_id in self.job_and_sub_jobs_structure.keys():
            for id in self.job_and_sub_jobs_structure[collection_id]:
                if job_id and job_id != id:
                    continue
                job_info = self.__get_job(id)
                for sub_job_id in job_info['sub_jobs']:
                    if not sub_jobs_to_ignore or not sub_job_id in sub_jobs_to_ignore:
                        sub_jobs_list.append(self.__get_job(sub_job_id))
        return sub_jobs_list
                          
    def get_job_and_sub_job_status_obj(self, job_id, completed_sub_jobs):
        json_job_list = dumps(self.ayfie.get_jobs())
        self.__gen_auxilary_data_structure(json_job_list)
        sub_jobs = self.__get_list_of_sub_jobs(json_job_list, job_id, completed_sub_jobs)
        return {"job": self.__get_job(job_id), "sub_jobs": sub_jobs}
        
    def get_job_status(self, job_id):
        json_job_list = dumps(self.ayfie.get_jobs())
        self.__gen_auxilary_data_structure(json_job_list)
        return(self.__get_job(job_id)['state']) 
        
    def _print_job_and_sub_jobs(self, job_info, verbose=False):
        if verbose:
            print(dumps(self.ayfie.get_job(job_info["id"]), indent=3))
        else:
            print(f'   -job: {job_info["state"]} {job_info["id"]} {job_info["type"]} {job_info["time_stamp"].replace("T", " ").split(".")[0]}')
        for sub_job_id in job_info['sub_jobs']:
            if verbose:
                print(dumps(self.ayfie.get_job(sub_job_id), indent=3))
            else:
                sub_job_info = self.__get_job(sub_job_id)
                print(f'       -sub-job: {sub_job_info["state"].ljust(10)} {sub_job_id.ljust(10)} {sub_job_info["type"].ljust(22)}  {sub_job_info["clock_time"].ljust(8)}')

    def wait_for_job_to_finish(self, job_id, timeout=None):
        start_time = None
        while True:
            job_status = self.get_job_status(job_id)
            if start_time == None and job_status in ['RUNNING','SUCCEEDED','FAILURE']:
                start_time = time()
            elapsed_time = int(time() - start_time)
            if job_status in ['SUCCEEDED', 'FAILURE']:
                break
            if timeout and elapsed_time > timeout:
                raise TimeoutError(f"Waited for more than {timeout} seconds for job to finish")
            sleep(1)
        return elapsed_time
        
    def print_jobs_overview(self, json_job_list=None, col_id=None, job_id=None, latest_job_only=False, verbose=False):
        if not json_job_list:
            json_job_list = dumps(self.ayfie.get_jobs()) 
        self.__gen_auxilary_data_structure(json_job_list)
        for collection_id in self.job_and_sub_jobs_structure.keys():
            if col_id and col_id != collection_id:
                continue
            latest_job_id = None
            for id in self.job_and_sub_jobs_structure[collection_id]:
                if job_id and job_id != id:
                    continue
                job_info = self.__get_job(id)
                if not latest_job_id:
                    latest_job_id = id
                elif job_info["time_stamp"] > self.__get_job(latest_job_id)["time_stamp"]:
                    latest_job_id = id
                if not latest_job_only:
                    self._print_job_and_sub_jobs(job_info, verbose)
            if latest_job_only:
                self._print_job_and_sub_jobs(self.__get_job(latest_job_id), verbose)
               
class Config():

    def __init__(self, config):
        self.silent_mode      = self.__get_item(config, 'silent_mode', True)
        self.config_source    = self.__get_item(config, 'config_source', None)
        self.server           = self.__get_item(config, 'server', '127.0.0.1')
        self.port             = self.__get_item(config, 'port', '80')
        self.api_version      = self.__get_item(config, 'api_version', 'v1')
        self.col_name         = self.__get_item(config, 'col_name', None)
        self.csv_mappings     = self.__get_item(config, 'csv_mappings', None)
        self.progress_bar     = self.__get_item(config, 'progress_bar', True)
        self.document_update  = self.__get_item(config, 'document_update', False)
        self.processing       = self.__get_item(config, 'processing', False)
        self.reporting        = self.__get_item(config, 'reporting', False)
        self.schema           = self.__get_item(config, 'schema', False)
        self.__init_schema(self.schema)
        self.feeding          = self.__get_item(config, 'feeding', False)
        self.__init_feeder(self.feeding)
        self.clustering       = self.__get_item(config, 'clustering', False)
        self.__init_clustering(self.clustering)
        self.classification   = self.__get_item(config, 'classification', False)
        self.__init_classification(self.classification)
        self.documents_update = self.__get_item(config, 'documents_update', False)
        self.__init_documents_update(self.documents_update)
        self.search           = self.__get_item(config, 'search', False)
        self.__init_search(self.search)
        self.regression_testing = self.__get_item(config, 'regression_testing', False)
        self.__init_report(self.reporting)
        self.reporting        = self.__get_item(config, 'reporting', False)
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
            if item in config and (config[item] or type(config[item]) in [bool, int]):
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
        self.data_source              = self.__get_item(feeding, 'data_source', None)
        if self.data_source and not isabs(self.data_source):
            self.data_source = join(path_split(self.config_source)[0], self.data_source)
        self.csv_mappings             = self.__get_item(feeding, 'csv_mappings', self.csv_mappings)
        self.data_type                = self.__get_item(feeding, 'data_type', AUTO)
        self.prefeeding_action        = self.__get_item(feeding, 'prefeeding_action', NO_ACTION)
        self.format                   = self.__get_item(feeding, 'format', {})
        self.encoding                 = self.__get_item(feeding, 'encoding', AUTO)
        self.batch_size               = self.__get_item(feeding, 'batch_size', MAX_BATCH_SIZE)
        self.document_type            = self.__get_item(feeding, 'document_type', None)
        self.preprocess               = self.__get_item(feeding, 'preprocess', None)
        self.ayfie_json_dump_file     = self.__get_item(feeding, 'ayfie_json_dump_file', None)
        self.report_doc_ids           = self.__get_item(feeding, 'report_doc_ids', False)
        self.second_content_field     = self.__get_item(feeding, 'second_content_field', None)
        self.file_picking_list        = self.__get_item(feeding, 'file_picking_list', None)
        self.id_picking_list          = self.__get_item(feeding, 'id_picking_list', [])
        self.file_copy_destination    = self.__get_item(feeding, 'file_copy_destination', None)
        self.feeding_disabled         = self.__get_item(feeding, 'feeding_disabled', False)
        self.report_fed_doc_size      = self.__get_item(feeding, 'report_fed_doc_size', False)       
        self.max_doc_characters       = self.__get_item(feeding, 'max_doc_characters', 0)
        self.min_doc_characters       = self.__get_item(feeding, 'min_doc_characters', 0)       
        self.max_doc_size             = self.__get_item(feeding, 'max_doc_size', 0)
        self.min_doc_size             = self.__get_item(feeding, 'min_doc_size', 0)
        self.file_type_filter         = self.__get_item(feeding, 'file_type_filter', None)
        self.file_extension_filter    = self.__get_item(feeding, 'file_extension_filter', None)
        self.rep_total_size_and_numb  = self.__get_item(feeding, 'rep_total_size_and_numb', None)
        self.feeding_report_time      = self.__get_item(feeding, 'report_time', False)
        self.report_doc_error         = self.__get_item(feeding, 'report_doc_error', False)
        self.no_processing            = self.__get_item(feeding, 'no_processing', False)
        
    def __init_processing(self, processing): 
        self.thread_min_chunks_overlap= self.__get_item(processing, 'thread_min_chunks_overlap', None)
        self.near_duplicate_evaluation= self.__get_item(processing, 'near_duplicate_evaluation', None)
        self.processing_report_time   = self.__get_item(processing, 'report_time', False)

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
        self.clustering_filters       = self.__get_item(clustering, 'filters', [])
        self.clustering_output_field  = self.__get_item(clustering, 'outputField', '_cluster')
        self.clustering_report_time   = self.__get_item(clustering, 'report_time', False)

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
        
    def __init_documents_update(self, documents_update):       
        self.doc_update_query         = self.__get_item(documents_update, 'query', "*")
        self.doc_update_action        = self.__get_item(documents_update, 'update_action', ADD_VALUES)
        self.doc_update_field         = self.__get_item(documents_update, 'field', 'trainingClass')
        self.doc_update_values        = self.__get_item(documents_update, 'values', [])
        self.doc_update_filters       = self.__get_item(documents_update, 'filters', [])
        self.numb_of_docs_to_update   = self.__get_item(documents_update, 'numb_of_docs', "ALL")
        self.doc_update_report_time   = self.__get_item(documents_update, 'report_time', False)
        
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
        
    def __init_report(self, report):
        self.report_output_destination= self.__get_item(report, 'output_destination', "terminal")
        self.report_verbose           = self.__get_item(report, 'verbose', False)
        self.report_jobs              = self.__get_item(report, 'jobs', False)
        self.report_logs_source       = self.__get_item(report, 'logs_source', False)
        self.report_retrieve_logs     = self.__get_item(report, 'retrieve_logs', False)
        
    def __init_regression_testing(self, regression_testing):
        self.upload_config_dir        = self.__get_item(regression_testing, 'upload_config_dir', None)
        self.query_config_dir         = self.__get_item(regression_testing, 'query_config_dir', None)
        self.server_settings          = self.__get_item(regression_testing, 'server_settings', "override_config")

    def __inputDataValidation(self):
        if self.server != OFF_LINE and (not self.regression_testing):
            if not self.col_name:
                raise ConfigError('Mandatory input parameter collection name (col_name) has not been given') 
        if self.report_logs_source and self.report_retrieve_logs: 
            raise ConfigError('Either analyze existing file or produce new ones, both is not possible')  
        if self.no_processing and self.processing:
            raise ConfigError('Feeding option no_processing cannot be combined with a processing confiuration')  
            
class TestExecutor():

    def __init__(self, config, use_master_config_settings=True):
        self.config = config
        self.use_master_config_settings = use_master_config_settings
        
    def __execute_config(self, config_file_path):
        if getsize(config_file_path) > 99999:
            return
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

        if "search" in config:
            if "search_query_file" in config["search"]:
                config["search"]['search_query_file'] = self.__get_file_path(
                        config_file_path, config["search"]['search_query_file'])
                        
        if "reporting" in config:
            if "logs_source" in config["reporting"]:
                config["reporting"]['logs_source'] = self.__get_file_path(
                        config_file_path, config["reporting"]['logs_source'])

        temp_config_file = 'tmp.cfg'
        with open(temp_config_file, 'wb') as f:
            f.write(dumps(config).encode('utf-8'))
        try:
            run_cmd_line(['ayfieExpress.py', temp_config_file])
        except Exception as e:
            print(f'TEST ERROR: "{config_file_path}": {str(e)}')
            raise
        
    def __retrieve_configs_and_execute(self, root_dir):
        for config_file_path in FileHandlingTools().get_next_file(root_dir, ["data", "query", "disabled", "logs"]):
            self.__execute_config(config_file_path)

    def __get_file_path(self, root_path, leaf_path):
        if isabs(leaf_path):
            return leaf_path
        return join(path_split(root_path)[0], leaf_path)

    def run_tests(self):
        if self.config.upload_config_dir:
            config_dir = self.__get_file_path(self.config.config_source, self.upload_config_dir)
            self.__retrieve_configs_and_execute(config_dir)
        if self.config.query_config_dir:
            config_dir = self.__get_file_path(self.config.config_source, self.config.query_config_dir)
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
        data_source = None
        if self.config.feeding:
            data_source = DataSource(self.config)
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
        self.document_updater = DocumentUpdater(self.config)
        self.reporter = Reporter(self.config)       
        
    def run_config(self):
        if self.config.regression_testing:
            if self.config.server_settings == OVERRIDE_CONFIG:
                use_master_settings = True
            elif self.config.server_settings == RESPECT_CONFIG:
                use_master_settings = False
            else:
                raise ValueError(f"'{self.config.server_settings}' is not a valid value for regression-testing.server_settings")
            testExecutor = TestExecutor(self.config, use_master_settings)
            testExecutor.run_tests()
            return
            
        if self.config.server != OFF_LINE:
            if self.config.data_source:
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
                    if self.config.no_processing:
                        self.feeder.feed_documents_and_commit()
                    else:
                        self.feeder.feed_documents_commit_and_process()
                else:
                    raise ConfigError('Cannot feed to non-existing collection')
            else:
                if not self.feeder.collection_exists():
                    raise ConfigError(f'There is no collection "{self.config.col_name}"')
                if self.config.processing:
                    self.feeder.process()

            if self.config.documents_update:
                self.document_updater.do_updates()
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
                    testExecutor = TestExecutor(self.config, self.config.server_settings)
                    testExecutor.process_test_result(result)
                elif self.config.search_result == RETURN_RESULT:   
                    return result
                elif self.config.search_result == DISPLAY_RESULT:
                    pretty_print(result)
                elif self.config.search_result == ID_LIST_RESULT: 
                    pretty_print([doc["document"]["id"] for doc in result["result"]])
                elif self.config.search_result.startswith('save:'):
                    with open(self.config.search_result[len(SAVE_RESULT):], 'wb') as f:
                        f.write(dumps(result, indent=4).encode('utf-8'))
                else:    
                    msg = '"search_result" must be "{DISPLAY_RESULT}", "{RETURN_RESULT}", "{ID_LIST_RESULT}", "{SAVE_RESULT}<file path>" or int'
                    raise ValueError(msg)
                                 
        if self.config.reporting:
            self.reporter.do_reporting()
            
                  
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
    run_cmd_line(sys.argv)
    
    
    