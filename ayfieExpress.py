from json import loads, dumps
from json.decoder import JSONDecodeError
from time import sleep, time
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
from copy import deepcopy
import logging
import requests
import sys
import csv

max_int_value = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int_value)
        break
    except OverflowError:
        max_int_value = int(max_int_value/2)

try:
    import PyPDF2
    pdf_is_suported = True
except:
    pdf_is_suported = False

log = logging.getLogger(__name__)

INSPECTOR             = "inspector"
SOLR                  = "solr"
ELASTICSEARCH         = "elasticsearch"

MAX_LOG_ENTRY_SIZE    = 1000

ROOT_SUB_DIR          = 'ayfieExpress_output'
UNZIP_DIR             = join(ROOT_SUB_DIR, 'unzipDir')
DATA_DIR              = join(ROOT_SUB_DIR, 'dataDir')
LOG_DIR               = join(ROOT_SUB_DIR, 'log')
DOWNLOAD_DIR          = join(ROOT_SUB_DIR, 'inspector_download')
TMP_LOG_UNPACK_DIR    = join(ROOT_SUB_DIR, 'temp_log_unpacking')

OFF_LINE              = "off-line"
OVERRIDE_CONFIG       = "override_config"
RESPECT_CONFIG        = "respect_config"

HTTP_GET              = 'GET'
HTTP_PUT              = 'PUT'
HTTP_POST             = 'POST'
HTTP_PATCH            = 'PATCH'
HTTP_DELETE           = 'DELETE'
HTTP_VERBS            = [HTTP_POST, HTTP_GET, HTTP_PUT, HTTP_PATCH, HTTP_DELETE]
JOB_TYPES             = ['PROCESSING', 'CLUSTERING', 'SAMPLING', 'CLASSIFICATION']
JOB_STATES            = ['RUNNING', 'SUCCEEDED']
FIELD_TYPES           = ['TEXT_EN', 'KEYWORD', 'ENTITY', 'EMAIL_ADDRESS', 'INTEGER',
                         'DOUBLE', 'BOOLEAN', 'DATE', 'PATH']
FIELD_ROLES           = ['EMAIL_SUBJECT', 'EMAIL_BODY', 'EMAIL_SENT_DATE',
                         'EMAIL_SENDER', 'EMAIL_TO', 'EMAIL_CC', 'EMAIL_BCC',
                         'EMAIL_ATTACHMENT', 'EMAIL_CONVERSATION_INDEX',
                         'EMAIL_HEADEREXTRACTION', 'DEDUPLICATION', 'CLUSTERING',
                         'HIGHLIGHTING', 'FAST_HIGHLIGHTING']
COL_TRANSITION        = ['DELETE', 'RECOVER', 'COMMIT', 'PROCESS']
COL_STATES            = ['EMPTY', 'IMPORTING', 'COMMITTING', 'COMMITTED', 'PROCESSING',
                         'PROCESSED', 'ABORTED', 'DELETING']
CLASSIFIER_TRANSITION = ['TRAIN']
CLASSIFIER_STATES     = ['INITIALIZED', 'TRAINING', 'TRAINED', 'INVALID']

DOCUMENT_TYPES        = ['email', 'default', 'autodetect']
PREPROCESS_TYPES      = ['rfc822', 'NUIX', 'IPRO']

COL_APPEAR            = 'appear'
COL_DISAPPEAR         = 'disappear'
COL_EVENTS            = [COL_APPEAR, COL_DISAPPEAR]
ITEM_TYPES            = ['collections', 'classifiers', 'jobs:jobinstances']

MAX_BATCH_SIZE        = 1000
MAX_PAGE_SIZE         = 10000

DEL_ALL_COL           = "delete_all_collections"
DEL_COL               = "delete_collection"
CREATE_MISSING_COL    = "create_collection_if_not_exists"
RECREATE_COL          = "recreate_collection"
NO_ACTION             = "no_action"
PRE_ACTIONS           = [NO_ACTION, DEL_ALL_COL, DEL_COL, RECREATE_COL, CREATE_MISSING_COL]

AUTO                  = "auto"
AYFIE                 = "ayfie"
AYFIE_RESULT          = "ayfie_result"
CSV                   = "csv"
JSON                  = "json"
PDF                   = "pdf"
WORD_DOC              = "docx"
XLSX                  = "xlsx"
TXT                   = "txt"
XML                   = 'xml'
DATA_TYPES            = [AYFIE, AYFIE_RESULT, JSON, CSV, PDF, TXT]
JSON_TYPES            = [AYFIE, AYFIE_RESULT, JSON]
ZIP_FILE              = 'zip'
TAR_FILE              = 'tar'
GZ_FILE               = 'gz'
BZ2_FILE              = 'bz2'
_7Z_FILE              = '7z'
RAR_FILE              = 'rar'
ZIP_FILE_TYPES        = [ZIP_FILE, TAR_FILE, GZ_FILE, BZ2_FILE, _7Z_FILE, RAR_FILE]
TEXT_ENCODINGS        = [BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE]

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
    BOM_UTF8: 'utf-8-sig',  # <== This is required for CSV DictReader to work
    BOM_UTF16_LE: 'utf_16_le',
    BOM_UTF16_BE: 'utf_16_be',
    BOM_UTF32_LE: 'utf_32_le',
    BOM_UTF32_BE: 'utf_32_be'
}

MAX_CONNECTION_RETRIES = 100
MAX_PAUSE_BEFORE_RETRY = 120

RETURN_RESULT          = 'return'
DISPLAY_RESULT         = 'display'
QUERY_AND_HITS_RESULT  = "query_and_numb_of_hits"
ID_LIST_RESULT         = 'id_list'
ID_AND_CLASSIFER_RESULT= 'id_and_classifier_score_list'
SAVE_RESULT            = 'save:'
RESULT_TYPES           = [RETURN_RESULT, DISPLAY_RESULT, QUERY_AND_HITS_RESULT, ID_LIST_RESULT, ID_AND_CLASSIFER_RESULT]

JOB_POLLING_INTERVAL   = 10

CLEAR_VALUES           = "clear"
REPLACE_VALUES         = "replace"
ADD_VALUES             = "add"
DOC_UPDATE_ACTIONS     = [CLEAR_VALUES, REPLACE_VALUES, ADD_VALUES]
K_FOLD_FIELD           = "k_fold_data_set"

COL_EVENT              = 'col_event'
COL_STATE              = 'col_state'
CLASSIFIER_STATE       = 'classifier_state'
WAIT_TYPES             = [COL_EVENT, COL_STATE, CLASSIFIER_STATE]


class HTTPError(IOError):
    pass

class ConfigError(Exception):
    pass

class AutoDetectionError(Exception):
    pass

class DataFormatError(Exception):
    pass
    
class BadStateError(Exception):
    pass

def pretty_print(json):
    if type(json) is str:
        if not len(json):
            raise ValueError('json string cannot be empty string')
        json = loads(json)
    print(dumps(json, indent=4))
    
    
class SearchEngine:

    def __init__(self, server, port, base_endpoint, headers):
        self.server = server
        try:
            self.port = str(int(port))
        except ValueError:
            raise ValueError(f"Value '{port}' is not a valid port number")
        self.protocol = "http"
        if self.port == '443':
            self.protocol = "https"
        self.base_endpoint = base_endpoint
        self.headers = headers
        self.statusCodes = self._getHTTPStatusCodes()
        
    def _getHTTPStatusCodes(self):
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
             
    def _get_endpoint(self, path, parameters):
        if parameters:
             parameters = f"?{ parameters}"
        if self.base_endpoint and self.base_endpoint[-1] != "/":
            self.base_endpoint += "/"
        return f'{self.protocol}://{self.server}:{self.port}/{self.base_endpoint}{path}{parameters}'  

    def _validateInputValue(self, input_value, allowed_values):
        if input_value not in allowed_values:
            allowed_values_str = '", "'.join(allowed_values)
            error_msg = f'Unknown input value "{input_value}".'
            error_msg += f'Allowed values are: "{allowed_values_str}".'
            raise ValueError(error_msg)

    def _gen_req_log_msg(self, verb, endpoint, data, headers):
        if len(str(data)) > MAX_LOG_ENTRY_SIZE:
            data = {"too much":"data to log"}
        else:
            data = dumps(data)
        return f"Request: {verb} {endpoint} json={data} headers={headers}"

    def _gen_response_err_msg(self, response_obj, ayfieStatus):
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
        
    def _gen_non_ayfie_code_msg(self, code):
        ayfieCodes = ', '.join(list(map(str, self.statusCodes.keys())))
        return f"Unexpected HTTP status code {code}, Expected: {ayfieCodes}"

    def _get_id_from_header(self, headers):
        if 'location' in headers:
            url = headers['location']
            p = f"^https?://.*/{self.base_endpoint}(collections|jobs)/(.*$)"
            m = match(p, url)
            if m:
                return m.group(2)
        return None
        
    def _log_attempt_and_go_to_sleep(self, tries, error_msg, go_to_sleep): 
        sleep_msg = ""
        if go_to_sleep:
            sleep_interval = tries * 5
            if sleep_interval > MAX_PAUSE_BEFORE_RETRY:
                sleep_interval = MAX_PAUSE_BEFORE_RETRY 
            sleep(sleep_interval)
            sleep_msg = f". Trying again in {sleep_interval} seconds"
        msg = f"Connection issue: {error_msg}{sleep_msg}"
        log.debug(msg)
        print(msg)
        
    def _response_format_converter(self, response_obj):
        return response_obj
        
    def _get_response_object(self, response, verb, endpoint):
        try:
            response_obj = loads(response.text) if len(response.text) else {}
        except JSONDecodeError:
            pattern = "<title>(Error )(\d{3,3}) (.*)</title>"
            m = search(pattern, response.text)
            if m:
                response_obj = {'error': m.group(3)}
            else:
                raise HTTPError(response.text)
        except:
            raise HTTPError(response.text)
        return self._response_format_converter(response_obj)
        
    def _execute(self, path, verb, data={}, parameters=""):
        self._validateInputValue(verb, HTTP_VERBS)
        request_function = getattr(requests, verb.lower())
        endpoint = self._get_endpoint(path, parameters)
        log.debug(self._gen_req_log_msg(verb, endpoint, data, self.headers))
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
                self._log_attempt_and_go_to_sleep(tries, str(e), go_to_sleep=True)
            except MemoryError:
                self._log_attempt_and_go_to_sleep(tries, "Out of memory - giving up!", go_to_sleep=False)
                raise
            except Exception as e:
                self._log_attempt_and_go_to_sleep(tries, "Unknown/unhandled error reason", go_to_sleep=False) 
                raise
         
        id_from_headers = self._get_id_from_header(response.headers)
        log.debug(f'HTTP response status: {response.status_code}')
        if response.status_code in self.statusCodes.keys():
            ayfieStatus = self.statusCodes[response.status_code]
        else:
            ayfieStatus = {
                "code": f"{response.status_code}",
                "description": "Not a ayfie recognized response code"
            }
            log.warning(self._gen_non_ayfie_code_msg(response.status_code))
        response_obj = self._get_response_object(response, verb, endpoint)
        if response.status_code >= 400:
            error_msg = self._gen_response_err_msg(response_obj, ayfieStatus)
            log.debug(error_msg)
            if response.status_code >= 500:
                with open(f"failed-batch-{response.status_code}-error.json", "wb") as f:
                    f.write(dumps(data).encode('iso-8859-1'))
            raise HTTPError(error_msg)
        if id_from_headers and not 'batches' in path:
            return id_from_headers
        return response_obj
        
    def _get_item_picking_endpoint_and_parameters(self, itemName):
        return itemName, ""
    
    def _get_all_items_of_an_item_type(self, item_type):
        self._validateInputValue(item_type, ITEM_TYPES)
        itemNames = item_type.split(':')
        endpoint, parameters = self._get_item_picking_endpoint_and_parameters(itemNames[0])
        result_field = itemNames[1] if len(itemNames) > 1 else itemNames[0]
        result = self._execute(endpoint, HTTP_GET, parameters=parameters)
        if '_embedded' in result:
            if result_field in result['_embedded']:
                return result['_embedded'][result_field]
        return []
        
    def _wait_for_collection_event(self, col_id, event, timeout=None):
        sleep(1)
        
    def _wait_for_collection_state(self, col_id, state, timeout):
        sleep(1)

    def _wait_for_collection_to_appear(self, col_id, timeout=120):
        sleep(1)

    def _wait_for_collection_to_disappear(self, col_id, timeout=120):
        sleep(1)
              
    ######### Collection Management #########

    def create_collection(self, col_name, col_id=None):
        raise NotImplementedError
        
    def create_collection_and_wait(self, col_name, col_id=None, timeout=120):
        id_string = col_id if col_id else col_name
        m = match("^[0-9a-z_-]+$", id_string)
        if not m:
            raise ValueError(f"Trying to create collection with illegal collection id string: '{id_string}'")
        col_id = self.create_collection(col_name, col_id)
        self._wait_for_collection_to_appear(col_id, timeout)
        return col_id 
        
    def get_collections(self):
        return self._get_all_items_of_an_item_type('collections')   

    def delete_collection(self, col_id):
        raise NotImplementedError
 
    def delete_collection_and_wait(self, col_id, timeout=120):
        self.delete_collection(col_id)
        self._wait_for_collection_to_disappear(col_id, timeout)
        
    def delete_all_collections_and_wait(self, timeout=120):
        col_ids = self.get_collection_ids()
        for col_id in col_ids:
            self.delete_collection(col_id)
        for col_id in col_ids:
            self._wait_for_collection_to_disappear(col_id, timeout)

    def get_collection(self, col_id):
        raise NotImplementedError

    def get_collection_state(self, col_id):
        raise NotImplementedError

    def get_collection_schema(self, col_id):
        raise NotImplementedError
        
    def add_collection_schema_field(self, col_id, schema_field_config):
         raise NotImplementedError
         
    def get_collection_schema_field(self, col_id, field_name):
        raise NotImplementedError
        
    def exists_collection_schema_field(self, col_id, field_name):
        schema = self.get_collection_schema(col_id)
        if field_name in schema["fields"]:
            return True
        return False

    def commit_collection(self, col_id):
        raise NotImplementedError

    def commit_collection_and_wait(self, col_id, timeout=300):
        raise NotImplementedError

    def feed_collection_documents(self, col_id, documents):
        raise NotImplementedError

    def get_collection_documents(self, col_id, page_size=100):
        raise NotImplementedError

    def get_collection_size(self, col_id):
        raise NotImplementedError
        
    def _get_collections_item(self, item, col_name=None):
        return [collection[item] for collection in self.get_collections() if not col_name or collection['name'] == col_name]
            
    def get_collection_names(self):
        return self._get_collections_item('name')

    def get_collection_ids(self, col_name=None):
        return self._get_collections_item('id', col_name)
        
    def get_collection_id(self, col_name):
        raise NotImplementedError
        
    def exists_collection_with_id(self, col_id):
        raise NotImplementedError

    def exists_collection_with_name(self, col_name):
        if col_name in self.get_collection_names():
            return True
        return False

    def meta_search(self, col_id, json_query):
        raise NotImplementedError

    def json_based_meta_concept_search(self, col_id, json_query):
        raise NotImplementedError

    def doc_search(self, col_id, json_query):
        raise NotImplementedError

    def search_collection(self, col_id, query, size=10, offset=0, filters=[], exclude=[], aggregations=[], scroll=False, meta_data=False):
        raise NotImplementedError
        
    def next_scroll_page(self, link):
        raise NotImplementedError
        
    def get_next_scroll_pages(self, page):
        raise NotImplementedError
 
    ######### Jobs #########

    def create_job(self, config):
        raise NotImplementedError
        
    def get_jobs(self):
        raise NotImplementedError
        
    def get_job(self, job_id, verbose=None):
         raise NotImplementedError

    def create_job_and_wait(self, config, timeout=None):
        raise NotImplementedError
        
    ######### Processing / Indexing #########

    def get_process_config(self, col_id):
        return {}

    def process_collection(self, col_id, config={}):
        return
    
    def process_collection_and_wait(self, col_id, config={}, timeout=None):
        return
        
        
class Solr(SearchEngine):

    def __init__(self, server, port, api_version=None):
        SearchEngine.__init__(self, server, port, "solr", {"Content-Type":"application/json"})
         
    def _response_format_converter(self, response_obj):
        converted_obj = None
        if "collections" in response_obj:
            collections = [{"id":col,"name":col} for col in response_obj["collections"]]
            converted_obj = {"_embedded": {"collections": collections}}
        elif "success" in response_obj:
            converted_obj = {}
        elif "error" in response_obj:
            converted_obj = {"error": response_obj["error"]["msg"]}
        elif "responseHeader" in response_obj and "status" in response_obj["responseHeader"]:
            if response_obj["responseHeader"]["status"] == 0:
                converted_obj = {}
        if converted_obj == None:
            raise NotImplementedError("'_response_format_converter()' not completed")
        return converted_obj
        
    def create_collection(self, col_name, col_id=None):
        parameters = f"name={col_name}&action=CREATE&numShards=2&replicationFactor=1"
        return self._execute("admin/collections", HTTP_POST, parameters=parameters)
      
    def delete_collection(self, col_id):   
        self._execute(f'admin/collections', HTTP_GET, parameters=f"action=DELETE&name={col_id}")
        
    def get_collection_schema(self, col_id):
        return self._execute(f'{col_id}/schema/fields', HTTP_GET)
        
    def add_collection_schema_field(self, col_id, schema_field_config):
        schema_field_config = {"add-field": {k:v for k,v in schema_field_config.items() if v != []}}
        path = f'{col_id}/schema/'
        return self._execute(path, HTTP_POST, schema_field_config)
            
    def get_collection_id(self, col_name):
        return col_name
        
    def meta_search(self, col_id, json_query):
        print('https://lucene.apache.org/solr/guide/7_1/json-request-api.html')
        return self._execute(f"{col_id}/select", HTTP_GET, parameters=f"q={json_query['query']}")
 
    def ping(self):
        endpoint = f"admin/ping"
        response = self._execute(HTTP_GET, endpoint)
        return response
       
    def feed_collection_documents(self, col_id, documents):
        return self._execute(f"{col_id}/update", HTTP_POST, documents)
        
    def commit_collection(self, col_id):
        self._execute(f"{col_id}/update", HTTP_GET, parameters="commit=true")
        
    def commit_collection_and_wait(self, col_id, timeout=300):
        self.commit_collection(col_id)
        
    def _get_item_picking_endpoint_and_parameters(self, item_name):
        return f"admin/{item_name}", "action=LIST&wt=json"

        
class Elasticsearch(Solr):

    def __init__(self, server, port, api_version=None):
        SearchEngine.__init__(self, server, port, "", {"Content-Type":"application/json"})
        
    def _get_response_object(self, response, verb, endpoint):
        response_obj = {}
        items = [{"id":line.split()[2],"name":line.split()[2]} for line in response.text.split('\n') if len(line.strip())]
        if verb == HTTP_GET and endpoint.endswith("_cat/indices"):
            response_obj = {"_embedded": {"collections": items}}
        return response_obj
        
    def create_collection(self, col_name, col_id=None):
        return self._execute(col_name, HTTP_PUT)
        
    def delete_collection(self, col_id):   
        self._execute(col_id, HTTP_DELETE)
       
    def feed_collection_documents(self, col_id, documents):
        return self._execute(f"{col_id}/update", HTTP_POST, documents)
        
        """
        POST http://path.to.your.cluster/myIndex/person/_bulk
        
        POST elasticsearch_domain/_bulk
        { "index": { "_index" : "index", "_type" : "type", "_id" : "id" } }
        { "A JSON": "document" }

        { "index":{} }
        { "name":"john doe","age":25 }
        { "index":{} }
        { "name":"mary smith","age":32 }
        """
        
    def _get_item_picking_endpoint_and_parameters(self, item_name):
        if item_name == "collections":
            return "_cat/indices", ""
        raise ValueError(f'Item name "{item_name}" is not supported')

        
class Inspector(SearchEngine):
  
    def __init__(self, server, port, api_version='v1'):
        SearchEngine.__init__(self, server, port,  f"ayfie/{api_version}", {'Content-Type':'application/hal+json'})

    def _wait_for(self, wait_type, id, good_value, bad_value=None, timeout=None):
        if not wait_type in WAIT_TYPES:
            raise ValueError(f"'{wait_type}' is not in {WAIT_TYPES}")
        if not id or not type(id) is str:
            raise ValueError(f"The 'id' must be a string, and not '{str(id)}'")
        if wait_type == COL_EVENT:
            self._validateInputValue(good_value, COL_EVENTS)
            negation = " not" if good_value == COL_APPEAR else " "
            err_msg  = f'Collection "{id}" still{negation} there after {timeout} seconds'
        elif wait_type == COL_STATE:
            self._validateInputValue(good_value, COL_STATES)
            err_msg  = f'Collection "{id}" still not in state {good_value} after {timeout} seconds'
        elif wait_type == CLASSIFIER_STATE:
            self._validateInputValue(good_value, CLASSIFIER_STATES)
            err_msg  = f'Classifier "{id}" still not in state {good_value} after {timeout} seconds'
        else:
            ValueError(f"Incident type {wait_type} does not exists")
           
        count_down = 0
        if timeout:
            count_down = timeout
        while (True):
            if wait_type == COL_EVENT:
                if self.exists_collection_with_id(id):
                    if good_value == COL_APPEAR:
                        return
                else:
                    if good_value == COL_DISAPPEAR:
                        return
            elif wait_type == COL_STATE: 
                current_state = self.get_collection_state(id)
                if good_value == current_state:
                    return
            elif wait_type == CLASSIFIER_STATE:
                current_state = self.get_classifier_state(id)
                if good_value == current_state:
                    return
                if bad_value and bad_value == current_state:
                    raise BadStateError(f"Classifier entered state '{current_state}'")
            
            if timeout and count_down < 0:
                raise TimeoutError(err_msg)
            sleep(5)
            count_down -= 1
            
    def _wait_for_collection_event(self, col_id, event, timeout=None):
        self._wait_for(COL_EVENT, col_id, event, None, timeout)
        
    def _wait_for_collection_state(self, col_id, state, timeout):
        self._wait_for(COL_STATE, col_id, state, None, timeout)

    def _wait_for_collection_to_appear(self, col_id, timeout=120):
        self._wait_for(COL_EVENT, col_id, COL_APPEAR, None, timeout)

    def _wait_for_collection_to_disappear(self, col_id, timeout=120):
        self._wait_for(COL_EVENT, col_id, COL_DISAPPEAR, None, timeout)

    def _change_collection_state(self, col_id, transition):
        self._validateInputValue(transition, COL_TRANSITION)
        data = {"collectionState": transition}
        return self._execute(f'collections/{col_id}', HTTP_PATCH, data)
 
    def _wait_for_collection_to_be_committed(self, col_id, timeout):
        self._wait_for(COL_STATE, col_id, 'COMMITTED', None, timeout)

    ######### Collection Management #########

    def create_collection(self, col_name, col_id=None):
        data = {'name': col_name}
        if col_id:
            data['id'] = col_id
        result = self._execute('collections', HTTP_POST, data) 
        return result

    def delete_collection(self, col_id):
        self._execute(f'collections/{col_id}', HTTP_DELETE)

    def delete_collection_and_wait(self, col_id, timeout=120):
        self.delete_collection(col_id)
        self._wait_for_collection_to_disappear(col_id, timeout)

    def get_collection(self, col_id):
        return self._execute(f'collections/{col_id}', HTTP_GET)

    def get_collection_state(self, col_id):
        return self.get_collection(col_id)["collectionState"]

    def get_collection_schema(self, col_id):
        return self._execute(f'collections/{col_id}/schema', HTTP_GET)
        
    def add_collection_schema_field(self, col_id, schema_field_config):
        schema_field_config = {k:v for k,v in schema_field_config.items() if v != []}
        path = f'collections/{col_id}/schema/fields'
        return self._execute(path, HTTP_POST, schema_field_config)
        
    def get_collection_schema_field(self, col_id, field_name):
        path = f'collections/{col_id}/schema/fields/{field_name}'
        return self._execute(path, HTTP_GET)

    def commit_collection(self, col_id):
        self._change_collection_state(col_id, "COMMIT")

    def commit_collection_and_wait(self, col_id, timeout=300):
        self.commit_collection(col_id)
        self._wait_for_collection_to_be_committed(col_id, timeout)

    def feed_collection_documents(self, col_id, documents):
        path = f'collections/{col_id}/batches'
        data = {'documents': documents}
        return self._execute(path, HTTP_POST, data)

    def get_collection_documents(self, col_id, page_size=100):
        path = f'collections/{col_id}/documents?size={page_size}'
        return self._execute(path, HTTP_GET)

    def get_collection_size(self, col_id):
        path = f'collections/{col_id}/documents?size=0'
        return self._execute(path, HTTP_GET)["meta"]["totalDocuments"]

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

    def meta_search(self, col_id, json_query):
        path = f'collections/{col_id}/search'
        return self._execute(path, HTTP_POST, json_query)

    def json_based_meta_concept_search(self, col_id, json_query):
        path = f'collections/{col_id}/search/concept'
        return self._execute(path, HTTP_POST, json_query)

    def doc_search(self, col_id, json_query):
        return self.meta_search(col_id, json_query)['result']

    def search_collection(self, col_id, query, size=10, offset=0, filters=[], exclude=[], aggregations=[], scroll=False, meta_data=False):
        json_query = {
            "highlight" : False,
            "sort": {
                "criterion": "_score",
                "order": "desc"
            },
            "minScore": 0.0,
            "exclude": exclude,
            "aggregations": aggregations,
            "filters": filters,
            "query": query,
            "scroll": scroll,
            "size": size,
            "offset": 0
        }
        if meta_data:
            return self.meta_search(col_id, json_query)
        else:
            return self.doc_search(col_id, json_query)
        
    def next_scroll_page(self, link):
        return self._execute("/".join(link.replace('//', "").split('/')[3:]), HTTP_GET)
        
    def get_next_scroll_pages(self, page):
        while True:
            try:
                link = page["_links"]["next"]["href"]
            except:
                break
            page = self.next_scroll_page(link)
            if "result" in page:
                number_of_docs = len(page["result"])
            elif "_embedded" in page:
                number_of_docs = len(page["_embedded"]["documents"])
            else:
                raise KeyError('Neither "_embedded" nor "result" in result page')
            if number_of_docs == 0:
                break
            yield page
 
    ######### Jobs #########

    def create_job(self, config):
        return self._execute('jobs', HTTP_POST, config)
        
    def get_jobs(self):
        return self._execute('jobs', HTTP_GET)
        
    def get_job(self, job_id, verbose=None):
        endpoint = f'jobs/{job_id}'
        if verbose:
            endpoint += "?verbose=true"
        return self._execute(endpoint, HTTP_GET)
        
    def _wait_for_job_to_finish(self, job_id, timeout=None, no_state_finish=False):
        start_time = None
        while True:
            job_report = self.get_job(job_id)
            if not "state" in job_report:
                if no_state_finish:
                    job_report["state"] = 'SUCCEEDED'
                else:
                    raise KeyError('No "state" in job report')
            job_status = job_report["state"]
            if start_time == None and job_status in ['RUNNING','SUCCEEDED','FAILURE']:
                start_time = time()
            elapsed_time = int(time() - start_time)
            if job_status in ['SUCCEEDED', 'FAILURE']:
                break
            if timeout and elapsed_time > timeout:
                raise TimeoutError(f"Waited for more than {timeout} seconds for job to finish")
            sleep(JOB_POLLING_INTERVAL)
        return elapsed_time

    def create_job_and_wait(self, config, timeout=None):
        job_id = self.create_job(config)
        return self._wait_for_job_to_finish(job_id, timeout)
        
    ######### Processing / Indexing #########

    def get_process_config(self, col_id):
        return {
            "collectionId" : col_id,
            "type" : "PROCESSING"
        }

    def process_collection(self, col_id, config={}):
        if not config:
            config = self.get_process_config(col_id)
        return self.create_job(config)    
    
    def process_collection_and_wait(self, col_id, config={}, timeout=None):
        if not config:
            config = self.get_process_config(col_id)
        return self.create_job_and_wait(config, timeout)
        
    ######### Sampling ######### 

    def start_sampling(self, col_id, config):
        path = f"collections/{col_id}/samples"
        return self._execute(path, HTTP_POST, config)
        
    def start_sampling_and_wait(self, col_id, config, timeout=600):
        job_id = self.start_sampling(col_id, config)
        self._wait_for_job_to_finish(job_id, timeout, no_state_finish=True)
        job_report = self.get_job(job_id)
        if not "sample" in job_report:
            raise DataFormatError('No "sample" item in job output')
        return [doc["id"] for doc in job_report["sample"]]  

    ######### Clustering #########

    def _get_clustering_config(self, col_id, clusters_per_level=None,
                              docs_per_cluster=None, max_recursion_depth=None,
                              gramian_singular_vectors=None, min_df=None,
                              max_df_fraction=None, gramian_threshold=None,
                              filters=[], output_field="_cluster"):
        config = {
            "collectionId" : col_id,
            "type" : "CLUSTERING",
            "filters" : filters,
            "outputField" : output_field
        }
        settings = {
            "clustersPerLevel" : clusters_per_level,
            "documentsPerCluster" : docs_per_cluster,
            "maxRecursionDepth" : max_recursion_depth,
            "gramianSingularVectors" : gramian_singular_vectors,
            "minDf" : min_df,
            "maxDfFraction" : max_df_fraction,
            "gramianThreshold" : gramian_threshold
        }
        settings = { k:v for k,v in settings.items() if v != None }
        if settings:
            config["settings"] = {"clustering" : settings}
        return config

    def create_clustering_job(self, col_id, **kwargs):
        config = self._get_clustering_config(col_id, **kwargs)
        return self.create_job(config)
        
    def create_clustering_job_and_wait(self, col_id, timeout=None, **kwargs):
        config = self._get_clustering_config(col_id, **kwargs)
        return self.create_job_and_wait(config, timeout)

    ######### Classification #########
 
    def _get_classifier_config(self, classifier_name=None, col_id=None, min_score=None,
                                      num_results=None, filters=None, output_field=None):
        config = {
            "collectionId" : col_id,
            "type" : "CLASSIFICATION",
            "filters" : filters,
            "outputField" : output_field
        }
        settings = {
            "classifierId": classifier_name,
            "minScore" : min_score,
            "numResults": num_results
        }
        settings = { k:v for k,v in settings.items() if v != None }
        if settings:
            config["settings"] = {"classification" : settings}
        return config     

    def create_classifier(self, classifier_name, col_id, training_field, min_score, num_results, filters):
        classifier_id = classifier_name  # For now at least
        data = {
            "name": classifier_name,
            "id": classifier_id,
            "collectionId": col_id,
            "trainingClassesField": training_field,
            "minScore": min_score,
            "numResults": num_results,
            "filters": filters
        }
        self._execute('classifiers', HTTP_POST, data)
        
    def get_classifiers(self):
        return self._get_all_items_of_an_item_type('classifiers')

    def get_classifier(self, classifier_name):
        return self._execute(f'classifiers/{classifier_name}', HTTP_GET)

    def get_classifier_names(self):
        return [classifier['name'] for classifier in self.get_classifiers()]
        
    def get_classifier_state(self, classifier_name):
        return self.get_classifier(classifier_name)["classifierState"]

    def delete_all_classifiers(self):
        classifier_names = self.get_classifier_names()
        for classifier_name in classifier_names:
            self.delete_classifier(classifier_name)

    def delete_classifier(self, classifier_name):
        return self._execute(f'classifiers/{classifier_name}', HTTP_DELETE)

    def update_document(self, col_id, doc_id, field, values):
        data = {field: values}
        endpoint = f'collections/{col_id}/documents/{doc_id}'
        self._execute(endpoint, HTTP_PATCH, data)

    def update_documents(self, col_id, doc_ids, field, values):
        for doc_id in doc_ids:
            self.update_document(col_id, doc_id, field, values) 

    def _get_document_update_by_query_config(self, query, filters, action, field, values=None):
        if not action in DOC_UPDATE_ACTIONS:
            raise ValueError(f"Parameter action cannot be {action}, but must be in {DOC_UPDATE_ACTIONS}")
        config = {"query" : {"query" : query}}
        if filters:
            config['query']['filters'] = filters
        if action == CLEAR_VALUES:
            config[action] = [field]
        else:
            if values:
                config[action] = {field: values}
            else:
                raise ValueError(f"The action '{action}' requires a list of values, not '{values}'")
        return config             
            
    def update_documents_by_query(self, col_id, query, filters, action, field, values=None):
        data = self._get_document_update_by_query_config(query, filters, action, field, values)
        endpoint = f'collections/{col_id}/documents'
        return self._execute(endpoint, HTTP_PATCH, data)
        
    def update_documents_by_query_and_wait(self, col_id, query, filters, action, field, values, timeout=None):
        job_id = self.update_documents_by_query(col_id, query, filters, action, field, values)
        return self._wait_for_job_to_finish(job_id, timeout)
        
    def train_classifier(self, classifier_name):
        data = {"classifierState": 'TRAIN'}
        return self._execute(f'classifiers/{classifier_name}', HTTP_PATCH, data)
 
    def wait_for_classifier_to_be_trained(self, classifier_name):
        self._wait_for("classifier_state", classifier_name, "TRAINED", "INVALID")
        
        
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
                            detected_encoding = BOM_MARKS_NAMES[pattern]
                            log.debug(f"'{file_path}' encoding auto detected to '{detected_encoding}'")
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
                dialect = csv.Sniffer().sniff(org_start_char_sequence)
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
            output_file_name = basename(file_path).replace(".gz", "")
            with open(join(unzip_dir, output_file_name), 'wb') as f, gzip_open(file_path, 'rb') as z:
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

    def get_next_file(self, root, dir_filter=None, extension_filter=None, min_size=0, max_size=0):
        items = listdir(root)
        files = [item for item in items if isfile(join(root, item))]
        dirs = [item for item in items if isdir(join(root, item))]
        for f in files:
            file_path = join(root, f)
            if min_size or max_size: 
                file_size = stat(file_path).st_size
                if min_size and file_size < min_size:
                    continue
                if max_size and file_size > max_size:
                    continue
            path_without_extension, extension = splitext(f)
            if extension_filter:
                if extension in extension_filter:
                    yield file_path
            else:
                yield file_path
        for dir in dirs:
            if dir_filter:
                if dir in dir_filter:
                    continue
            for f in self.get_next_file(join(root, dir), dir_filter, extension_filter, min_size, max_size):
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
            
    def get_random_path(self, path, name_prefix=""):
        if name_prefix:
            name_prefix += "-"
        return join(path, f"{name_prefix}{str(int(random() * 10000000))}")
         

class DataSource():

    def __init__(self, config, unzip_dir=UNZIP_DIR):
        self._init()
        self.config = config
        self.created_temp_data_dir = False
        if self.config.data_source == None:
            raise ValueError(f"Parameter 'data_source' is mandatory (but it can point to an empty source)")
        if self.config.data_source.startswith('http'):
            FileHandlingTools().recreate_directory(DOWNLOAD_DIR)
            self.config.data_source = WebContent().download(self.config.data_source, DOWNLOAD_DIR)         
        if not exists(self.config.data_source):
            raise ValueError(f"There is no file or directory called '{self.config.data_source}'")
        if isdir(self.config.data_source):
            self.data_dir = self.config.data_source
        elif isfile(self.config.data_source):
            self.data_dir = FileHandlingTools().get_random_path(DATA_DIR)
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
        
    def _init(self):
        self.doc_fields_key = "fields"

    def __del__(self):
        if self.created_temp_data_dir:
            delete_dir_tree(self.data_dir)
            
    def _get_file_type(self, file_path):
        file_type, encoding = FileHandlingTools().get_file_type(file_path)
        if file_type == CSV:
            if self.config.treat_csv_as_text:
                file_type = TXT
        return file_type, encoding

    def _unzip(self, file_type, file_path, unzip_dir):
        return FileHandlingTools().unzip(file_type, file_path, unzip_dir)

    def _convert_pdf_to_text(self, file_path):
        if not pdf_is_suported:
            raise ModuleNotFoundError('run "pip install PyPDF2"')
        with open(file_path, 'rb') as f:
            pdf = PyPDF2.PdfFileReader(f)
            pages = []
            for page_number in range(pdf.getNumPages()):
                pages.append(pdf.getPage(page_number).extractText())
            return ' '.join(pages)

    def _get_file_content(self, file_path_or_handle):
        if type(file_path_or_handle) is str:
            with open(file_path_or_handle, 'rb') as f:
                f = self._skip_file_byte_order_mark(f)
                content = f.read()
        else:
            f = self._skip_file_byte_order_mark(file_path_or_handle)
            content = f.read()
        if self.config.encoding == AUTO:
            if self.detected_encoding:
                try:
                    return content.decode(self.detected_encoding)
                except UnicodeDecodeError:
                    msg = f"Auto detected encoding {self.detected_encoding} failed for {f.name}, "
                    try:
                        content = content.decode('utf-8')
                        log.debug(msg + "using 'utf-8' instead")
                    except UnicodeDecodeError:
                        content = content.decode('iso-8859-1')
                        log.debug(msg + "using 'iso-8859-1' instead")
                    return content
            else:
                msg = f"No encoding was successfully auto detected for {f.name}, "
                try:
                    content = content.decode('utf-8')
                    log.debug(msg + "using 'utf-8' as fallback")
                except UnicodeDecodeError:
                    content = content.decode('iso-8859-1')
                    log.debug(msg + "using 'iso-8859-1' as fallback, after first trying 'utf-8' failed")
                return content
        else:
            return content.decode(self.config.encoding)
        raise Exception(f"Unable to resolve encoding for '{f.name}'")
        
    def _skip_file_byte_order_mark(self, f):
        bytes_to_skip = 0
        start_byte_sequence = f.read(4)
        for bom in TEXT_ENCODINGS:
            if start_byte_sequence[:len(bom)] == bom:
                bytes_to_skip = len(bom)
                break
        f.seek(0)
        f.read(bytes_to_skip)
        return f

    def _gen_id_from_file_path(self, file_path):
        return splitext(basename(file_path))[0]
        
    def _gen_doc_from_csv(self, csv_file, mappings, format):
        if not mappings:
            raise ConfigError("Configuration lacks a csv mapping table")
        for row in csv.DictReader(csv_file, **format):
            fields = {**{k:row[v] for k,v in mappings["fields"].items() if not type(v) is list},
                      **{k:[row[v[0]]] for k,v in mappings["fields"].items() if type(v) is list}}
            doc = {}
            if self.doc_fields_key:
                doc[self.doc_fields_key] = fields
            else:
                doc = fields
            doc["id"] = row[mappings['id']]
            yield doc
 
    def _get_document(self, file_path, auto_detected_file_type):
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
                if self.config.fallback_data_type:
                    data_type = self.config.fallback_data_type
                    log.error(f'File type detetection failed for "{file_path}", using fallback data type "{data_type}"')
                else:
                    log.error(f'File type detetection failed for "{file_path}" and no fallback data type set')
                    return None
        args = { 'mode': 'rb' }
        encoding = 'utf-8'
        if self.config.encoding == AUTO:
            if self.detected_encoding:
                encoding = self.detected_encoding
        else:
            encoding = self.config.encoding
        if data_type == CSV:
            args = {
                'mode': 'r',
                'newline': '',
                'encoding': encoding
            }
        with open(file_path, **args) as f:
            if data_type == CSV:
                for document in self._gen_doc_from_csv(f, self.config.csv_mappings, self.config.format):
                    yield document
            elif data_type in [AYFIE, AYFIE_RESULT]:
                data = self._get_file_content(f)
                if data_type == AYFIE:
                    self.config.format['root'] = 'documents'
                elif data_type == AYFIE_RESULT:
                    self.config.format['root'] = 'result'
                try:
                    for document in loads(data)[self.config.format['root']]:
                        if data_type == AYFIE_RESULT:
                            yield self._construct_doc(document["document"]["id"], document["document"]["content"])
                        else:
                            yield self._convert_doc(document) 
                except JSONDecodeError as e:
                    log.error(f"JSON decoding error for {file_path}: {str(e)}")
                    return None
            elif data_type == PDF:
                if self.config.ignore_pdfs: 
                    log.info("Dropping file as not configured to process pdf files")
                else:
                    yield self._construct_doc(self._gen_id_from_file_path(file_path), self._convert_pdf_to_text(file_path))
            elif data_type in [WORD_DOC, XLSX, XML]:
                log.info(f"Skipping '{file_path}' - conversion of {data_type} still not implemented")
            elif data_type == TXT:
                yield self._construct_doc(self._gen_id_from_file_path(file_path), self._get_file_content(file_path))
            else:
                log.error(f"Unknown data type '{data_type}'")
                
    def _get_document_fragments(self, document):
        if self.config.fragmented_doc_dir:
            if not exists(self.config.fragmented_doc_dir):
                makedirs(self.config.fragmented_doc_dir) 
        fields = [field for field in document['fields'] if field != "content"]
        if not "content" in document['fields']:
            raise DataFormatError("The document has no 'content' field under 'fields'")
        fragments = []
        fragment_length = self.config.doc_fragment_length
        content_length = len(document['fields']["content"])
        mini_fragments = document['fields']["content"].split()
        number_of_fragments = int(content_length / fragment_length)
        if number_of_fragments > self.config.max_doc_fragments:
            fragment_length = content_length / self.config.max_doc_fragments
        new_fragment = ""
        for mini_fragment in mini_fragments:
            if len(new_fragment) == 0:
                new_fragment = f" {mini_fragment}"
                continue
            if len(new_fragment) > fragment_length:
                fragments.append(new_fragment)
                new_fragment = f" {mini_fragment}"
            else:
                diff_now = fragment_length - len(new_fragment)
                diff_after = abs(fragment_length - (len(new_fragment) + len(mini_fragment)))
                if diff_after < diff_now:
                    new_fragment += f" {mini_fragment}"
                else:
                    fragments.append(new_fragment)
                    new_fragment = f" {mini_fragment}"
        if len(new_fragment):
            fragments.append(new_fragment)
        
        if len(fragments) >= 2:
            split_document = {"id":"", "fields":{}}
            for field in fields:
                split_document["fields"][field] = document["fields"][field]
            for i in range(len(fragments) - 1):
                split_document["id"] = f'{document["id"]}_{str(i)}'
                split_document["fields"]["content"] = fragments[i] + fragments[i+1]
                if self.config.fragmented_doc_dir:
                    with open(join(self.config.fragmented_doc_dir, split_document["id"] + ".json"), "wb") as f:
                        f.write(dumps(split_document, indent=4).encode("utf-8"))
                yield split_document
                   
    def extract_plain_text_email(self, email):
        boundary_marker = None
        is_boundary_definition_line = False
        is_a_new_message = False
        is_plain_text_message = False
        header_section = []
        is_header_section = True
        message_lines = [] 
        for line in email.split("\n"):
            if is_plain_text_message:
                if boundary_marker in line:
                    break
                else:
                    message_lines.append(line)
            if is_a_new_message:
                m = match("^\s*Content-Type:\s*text/plain;.*$", line)
                if m:
                    is_a_new_message = False
                    is_plain_text_message = True
            if is_boundary_definition_line:
                m = match("^\s*boundary=\"(.+)\"\s*$", line)
                if m:
                    boundary_marker = m.group(1)
                    is_boundary_definition_line = False
            if boundary_marker:
                if boundary_marker in line:
                    is_a_new_message = True
            elif match("Content-Type:\s*multipart/mixed;", line):
                is_boundary_definition_line = True
                is_header_section = False
            if is_header_section:
                header_section.append(line)
        return "\n".join(header_section + message_lines)
                
    def _do_multipart_processing(self, document):
        if self.config.convert_multipart_msg or self.config.flag_multipart_msg:
            if "multipart/mixed" in document["fields"]["content"]:
                if self.config.flag_multipart_msg:
                    print(f'Document with id "{document["id"]}" is a multipart email')
                if self.config.convert_multipart_msg:
                    document["fields"]["content"] = self.extract_plain_text_email(document["fields"]["content"])             
        return document
                        
    def get_documents(self):
        for document in self._get_unsplit_documents():
            document = self._do_multipart_processing(document)
            if self.config.doc_fragmentation:
                for document_fragment in self._get_document_fragments(document):
                    yield document_fragment
            else:
                yield document
                
    def _get_documents_from_zipped_files(self, file_type, file_path, unzip_dir):
        try:
            FileHandlingTools().recreate_directory(unzip_dir)
            log.debug(f"Created output directory '{unzip_dir}' for zip file '{file_path}'")
            self._unzip(file_type, file_path, unzip_dir)
            for unzipped_file in FileHandlingTools().get_next_file(unzip_dir, None, self.config.file_extension_filter, 
                                                                   self.config.min_doc_size, self.config.max_doc_size):
                file_type, self.detected_encoding = self._get_file_type(unzipped_file)
                log.debug(f"'{unzipped_file}' auto detected as '{file_type}'")
                if self.config.file_type_filter and not file_type in self.config.file_type_filter:
                    log.debug(f"'{file_path}' auto detected (2) as '{file_type} and not excluded due to file type filter: {self.config.file_type_filter}")
                    continue
                if file_type in ZIP_FILE_TYPES:
                    sub_unzip_dir = FileHandlingTools().get_random_path(unzip_dir)
                    for document in self._get_documents_from_zipped_files(file_type, unzipped_file, sub_unzip_dir):
                        yield document
                else:
                    for document in self._get_document(unzipped_file, file_type):
                        if document:
                            self.updateFileStatistics(unzipped_file)
                            yield document
        except:
            raise
        finally:
            FileHandlingTools().delete_directory(unzip_dir)
            log.debug(f"Deleting directory '{unzip_dir}'")

    def _get_unsplit_documents(self):
        self.file_size = {}
        self.file_paths = []
        for file_path in FileHandlingTools().get_next_file(self.data_dir, None, self.config.file_extension_filter, 
                                                           self.config.min_doc_size, self.config.max_doc_size):
            file_type, self.detected_encoding = self._get_file_type(file_path)
            log.debug(f"'{file_path}' auto detected as '{file_type}'")
            if self.config.file_type_filter and not file_type in self.config.file_type_filter:
                log.debug(f"'{file_path}' auto detected (1) as '{file_type}' and not excluded due to file type filter: {self.config.file_type_filter}")
                continue
            if file_type in ZIP_FILE_TYPES:
                for document in self._get_documents_from_zipped_files(file_type, file_path, self.unzip_dir):
                    yield document
            else:
                for document in self._get_document(file_path, file_type):
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
        
    def _drop_due_to_size(self, id, content):
        doc_size = 0
        if self.config.report_fed_doc_size or self.config.max_doc_characters or self.config.min_doc_characters:
            doc_size = len(content)
            if self.config.second_content_field:
                doc_size += doc_size
        if self.config.report_fed_doc_size:
            self._print(f"Doc size for document with id {id} is {doc_size} characters") 
        if ((self.config.max_doc_characters and (doc_size > self.config.max_doc_characters)) 
                     or (self.config.min_doc_characters and (doc_size < self.config.min_doc_characters))):   
            log.info(f"Document with id {id} dropped due to a size of {doc_size} characters")    
            return True
        return False
        
    def _construct_doc(self, id, content):
        if self._drop_due_to_size(id, content):
            return None
            
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
        if self.config.email_address_separator:  
            doc["fields"]['emailAddressSeparator'] = self.config.email_address_separator
        return doc
        
    def _convert_doc(self, document):
        return document

    
class SolrDataSource(DataSource):

    def _init(self):
        self.doc_fields_key = None

    def _construct_doc(self, id, content):
        if self._drop_due_to_size(id, content):
            return None
            
        doc = {
            "id": id,
            "content": content
        }
        if self.config.second_content_field:
            doc[self.config.second_content_field] = content
        if self.config.document_type:
            raise ConfigError("No document type supported for Solr, correct config") 
        if self.config.preprocess:
            raise ConfigError("No preprocessing supported for Solr, correct config")
        if self.config.email_address_separator:  
            raise ConfigError("No email sepearator supported for Solr, correct config")
        return doc
        
    def _convert_doc(self, document):
        converted_doc = document["fields"]
        converted_doc["id"] = document["id"]
        return converted_doc

        
class EngineConnector():
    
    def __init__(self, config, data_source=None):
        self.config = config
        self.data_source = data_source
        args = [self.config.server, self.config.port, self.config.api_version]
        if self.config.engine == INSPECTOR:
            self.engine = Inspector(*args)
        elif self.config.engine == SOLR:
            self.engine = Solr(*args)
        elif self.config.engine == ELASTICSEARCH:
            self.engine = Elasticsearch(*args)
        else:
            raise ValueError(f"Engine {self.config.engine} is not supported")
                 
    def _print(self, message, end='\n'):
        if not self.config.silent_mode:
            print(message, end=end)
            
    def get_doc_ids(self, col_id, query="*", limit=None, random_order=False, filters=[]):
        if not limit:
            limit = "ALL"
        docs_per_request = 10000
        if limit and limit != "ALL":
            limit = int(limit)
            if limit < docs_per_request:
                docs_per_request = limit
        result = self.engine.search_collection(col_id, query, docs_per_request, filters=filters,
                 exclude=["content", "term", "location", "organization", "person", "email"], 
                 scroll=True, meta_data=True)
        all_ids = [doc['document']['id'] for doc in result["result"]]
        for result in self.engine.get_next_scroll_pages(result):
            ids = [doc['document']['id'] for doc in result["result"]]
            all_ids += ids
            if limit != "ALL" and len(all_ids) >= int(limit):
                break
        if limit != "ALL" and limit < len(all_ids):
            all_ids = all_ids[:limit]
        if random_order:
            shuffle(all_ids)
        return all_ids
        

class SchemaManager(EngineConnector):

    def add_field_if_absent(self, schema_changes=None):
        col_id = self.engine.get_collection_id(self.config.col_name)
        if not schema_changes:
            schema_changes = self.config.schema_changes
        for schema_change in schema_changes:
            if not self.engine.exists_collection_schema_field(col_id, schema_change['name']):
                self.engine.add_collection_schema_field(col_id, schema_change)
                if not self.engine.exists_collection_schema_field(col_id, schema_change['name']):
                    raise ConfigError(f"Failed to add new schema field '{schema_change['name']}'")
                    

class DocumentRetriever(EngineConnector):

    def retrieve_documents(self):
        self.search_operation = False
        if self.config.doc_retrieval_query or self.config.doc_retrieval_filters:
            self.search_operation = True
        col_id = self.engine.get_collection_id(self.config.col_name)
        fields = self._get_fields_to_extract(col_id, self.config.doc_retrieval_fields)
        with open(self.config.doc_retrieval_output_file, mode='w', newline='', encoding="utf-8") as f:
            csv_writer = csv.DictWriter(f, fieldnames=fields, dialect=self.config.doc_retrieval_csv_dialect)
            if self.config.doc_retrieval_csv_header_row:
                csv_writer.writeheader()
            doc_count = -1
            for doc_batch in self._get_documents(col_id, self.config.doc_retrieval_query, self.config.doc_retrieval_filters):
                for document in doc_batch:
                    doc_count += 1
                    if self.config.doc_retrieval_max_docs:
                        if doc_count >= self.config.doc_retrieval_max_docs:
                            self._print(f"Aborting after {self.config.doc_retrieval_max_docs} documents as directed via configuration")
                            return
                    if doc_count and doc_count % self.config.doc_retrieval_report_frequency == 0:
                        self._print(f"So far {doc_count} documents have been extracted")
                    row = {}
                    for field in fields:
                        if self.search_operation:
                            value = document["document"].get(field, '')
                        else:
                            value = document.get(field, '')
                        if type(value) is list:
                            value = self.config.doc_retrieval_separator.join(value)
                        if type(value) is str:
                            row[field] = value.replace('\n', ' ').replace('\t', ' ')
                        else:
                            row[field] = value
                    csv_writer.writerow(row)
        tail = "."            
        if self.search_operation:
            tail = " matching configured query and/or filters."
        self._print(f"Done after extracting all {doc_count} documents{tail}")
                         
    def _get_documents(self, col_id, query="", filters=[]):
        if self.search_operation:
            result_page = self.engine.search_collection(col_id, query, self.config.doc_retrieval_batch_size, 0, filters,
                                                        self.config.doc_retrieval_exclude, scroll=True, meta_data=True)
            yield result_page["result"]
        else:
            result_page = self.engine.get_collection_documents(col_id, self.config.doc_retrieval_batch_size)
            yield result_page["_embedded"]["documents"]
        for result_page in self.engine.get_next_scroll_pages(result_page):
            if self.search_operation:
                yield result_page["result"]
            else:  
                yield result_page["_embedded"]["documents"]
 
    def _get_fields_to_extract(self, col_id, fields):
        if fields:
            return fields
        schema = self.engine.get_collection_schema(col_id)
        if "fields" in schema:
            if self.config.doc_retrieval_drop_underscore_fields:
                fields = [field for field in schema["fields"].keys() if not field.startswith("_")]
            else:
                fields = [field for field in schema["fields"].keys()]
        else:
            raise KeyError("No field 'fields' in schema: {}".format(schema))
        for field in self.config.doc_retrieval_exclude:
            fields.remove(field)
        return fields
        
            

class Feeder(EngineConnector):

    def collection_exists(self):
        if self.engine.exists_collection_with_name(self.config.col_name):
            return True
        else:
            return False

    def delete_collection_if_exists(self):
        if self.engine.exists_collection_with_name(self.config.col_name):
            log.info(f'Deleting collection "{self.config.col_name}"')
            col_id = self.engine.get_collection_id(self.config.col_name)
            self.engine.delete_collection_and_wait(col_id)

    def delete_all_collections(self):
        log.info('Deleting all collections')
        self.engine.delete_all_collections_and_wait()

    def create_collection_if_not_exists(self):
        if not self.engine.exists_collection_with_name(self.config.col_name):
            log.info(f'Creating collection "{self.config.col_name}"')
            col_id = self.config.col_name
            self.engine.create_collection_and_wait(self.config.col_name, col_id)

    def __send_batch(self):
        if self.config.ayfie_json_dump_file:
            with open(self.config.ayfie_json_dump_file, "wb") as f:
                try:
                    f.write(dumps({'documents': self.batch}, indent=4).encode('utf-8'))
                except Exception as e:
                    log.warning(f'Writing copy of batch to be sent to disk failed with the message: {str(e)}')    
        col_id = self.engine.get_collection_id(self.config.col_name)
        log.info(f'About to feed batch of {len(self.batch)} documents')
        return self.engine.feed_collection_documents(col_id, self.batch)

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
            doc_ids_msg = ""
            if self.config.report_doc_ids:
                doc_ids_msg = f" The batch contained these doc ids: {', '.join([doc['id'] for doc in self.batch])}"
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
                    sentence_start = "The last"
                err_message = f"{sentence_start} batch of {self.batch_size} docs fed to collection '{self.config.col_name}' failed: '{err_message}'" + doc_ids_msg
                self._print(err_message)
                log.error(err_message)
            else:
                self.doc_count += self.batch_size
                sentence_start = "So far"
                if is_last_batch:
                    sentence_start = "A total of" 
                self._print(f"{sentence_start} {self.doc_count} documents has been uploaded to collection '{self.config.col_name}'.{doc_ids_msg}")
        self.batch = []
        self.batch_size = 0
        
    def _get_unit_adapted_byte_figure(self, number_of_bytes):
        if number_of_bytes >= 1024 * 1024 * 1024:
            number_of_bytes /= 1024 * 1024 * 1024
            unit = "GB"
        elif number_of_bytes >= 1024 * 1024:
            number_of_bytes /= 1024 * 1024
            unit = "MB"
        elif number_of_bytes >= 1024:
            number_of_bytes /= 1024
            unit = "KB"
        else:
            return number_of_bytes, "bytes"
        return round(number_of_bytes, 3), unit
        
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
                if self.config.id_mappings:
                    if str(document["id"]) in self.config.id_mappings:
                        document["id"] = self.config.id_mappings[document["id"]]
                if id_picking_list:
                    if not (str(document["id"]) in id_picking_list):
                        continue
                self.batch.append(document)
                log.debug(f'Document with id "{document["id"]}" added to batch')
                self.batch_size = len(self.batch)
                if (self.batch_size >= self.config.batch_size) or (
                              self.config.max_docs_to_feed and (self.batch_size >= self.config.max_docs_to_feed)):
                    self.process_batch()
                if self.config.max_docs_to_feed and (self.doc_count >= self.config.max_docs_to_feed):
                    break
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
            total_file_size, unit = self._get_unit_adapted_byte_figure(total_file_size)  
            log_message = f"A total of {file_count} files and {total_file_size} {unit} were retrieved from disk" + log_message
            log.info(log_message)
            self._print(log_message)
        return elapsed_time
        
    def process(self):
        if self.config.progress_bar:
            return JobsHandler(self.config).job_processesing("PROCESSING", self.config.processing)
        else:
            col_id = self.engine.get_collection_id(self.config.col_name)
            self.engine.process_collection_and_wait(col_id)
        
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
        col_id = self.engine.get_collection_id(self.config.col_name)
        log.info(f'Fed {self.doc_count} documents. Starting to commit')
        self.engine.commit_collection_and_wait(col_id)
        log.info(f'Done committing')

    def feed_documents_commit_and_process(self):
        self.feed_documents_and_commit()
        processing_time = self.process()
        if self.config.feeding_report_time:
            self._print(f"The processing operation took {str(processing_time)} seconds")
        log.info(f'Done processing')


class Classifier(EngineConnector):

    def __create_and_train_classifiers(self, classification):
        if classification['name'] in self.engine.get_classifier_names():
            self.engine.delete_classifier(classification['name'])
        col_id = self.engine.get_collection_id(self.config.col_name) 
        self.engine.create_classifier(classification['name'], col_id, classification['training_field'],
                                     classification['min_score'], classification['num_results'], 
                                     classification['training_filters']) 
        self.engine.train_classifier(classification['name'])
        self.engine.wait_for_classifier_to_be_trained(classification['name'])

    def create_classifier_job_and_wait(self, classification):
        col_id = self.engine.get_collection_id(self.config.col_name)
        if "dummy" in classification:
            del classification["dummy"]
        settings = {
            "classification": {
                "classifierId": classification["name"],
                "minScore" : classification["min_score"],
                "numResults": classification["num_results"]
            } 
        }
        more_params = {
            "filters" : classification['execution_filters'],
            "outputField" : classification['output_field']
        }
        if self.config.progress_bar:
            return JobsHandler(self.config).job_processesing("CLASSIFICATION", settings, more_params)
        else:
            self.engine.classify_and_wait(col_id)  
            
    def do_classifications(self):
        for classification in self.config.classifications:
            self.__do_classification(classification)
            
    def __get_training_and_test_sets(self, classification):
        K = classification['k-fold']
        assert (type(K) is int and K > 0),"No k-fold value defined"
        col_id = self.engine.get_collection_id(self.config.col_name)
        query = f"_exists_:{classification['training_field']}"
        doc_ids = self.engine.get_doc_ids(col_id, query, random_order=True)
        q, r = divmod(len(doc_ids), K)
        doc_sets = [doc_ids[i * q + min(i, r):(i + 1) * q + min(i + 1, r)] for i in range(K)]
        for i in range(K):
            test_set_doc_ids = doc_sets[i]
            training_set_doc_ids = list(doc_sets)
            del training_set_doc_ids[i]
            yield [id for id_set in list(training_set_doc_ids) for id in id_set], test_set_doc_ids
            
    def __write_to_test_output_file(self, file_path, content="", create_file=False):
        if create_file:
            with open(file_path, "w") as f:
                f.write("""
                    <html>
                    <head>
                     <style type="text/css">
                    table {
                      border-collapse:collapse; 
                    }
                    table td th { 
                      padding:7px;  border:#4e95f4 1px solid;
                    }
                    th { 
                      padding:0 15px 0 15px;
                    }
                    td { 
                      padding:0 15px 0 15px;
                    }
                    table tr {
                      background: #b8d1f3;
                    }
                    table tr td:nth-child(odd) { 
                      background: #b8d1f3;
                    }
                    table tr td:nth-child(even) {
                      background: #dae5f4;
                    }
                    td {
                      text-align: right;
                    }
                    th {
                      text-align: left;
                    }
                    </style>
                    </head>
                    <body>
                """)
        with open(file_path, "a") as f:
            f.write(content)
            
    def __do_kfold_analyses(self, classification):
        SchemaManager(self.config).add_field_if_absent([
            {
                "name"       : K_FOLD_FIELD,
                "type"       : "KEYWORD",
                "list"       : True
            }
        ])
        known_categories = self.__get_known_categories(classification['training_field'])
        self.recall = {}
        self.precision = {}
        for known_category in known_categories:
            self.recall[known_category] = 0
            self.precision[known_category] = 0
        self.__write_to_test_output_file(classification['test_output_file'], create_file=True)
        for training_set, test_set in self.__get_training_and_test_sets(classification):
            DocumentUpdater(self.config).do_updates([
                {
                    "query": "*",
                    "action": CLEAR_VALUES,
                    "field": K_FOLD_FIELD
                },
                {
                    "query": "*",
                    "action": CLEAR_VALUES,
                    "field": classification['output_field']
                },
                {
                    "query": "*",
                    "action": REPLACE_VALUES,
                    "field": K_FOLD_FIELD,
                    "values": ["training"],
                    "filters": [{"field": "_id", "value": training_set}]
                },
                {
                    "query": "*",
                    "action": REPLACE_VALUES,
                    "field": K_FOLD_FIELD,
                    "values": ["verification"],
                    "filters": [{"field": "_id", "value": test_set}]
                }
            ])
            k_fold_classification = {
                "name"                 : "k_fold",
                "training_field"       : classification['training_field'],
                "min_score"            : classification['min_score'],
                "num_results"          : classification['num_results'],
                "training_filters"     : [{"field": K_FOLD_FIELD, "value": "training"}],
                "execution_filters"    : classification['execution_filters'],
                "output_field"         : classification['output_field']
            }
            self.__create_and_train_classifiers(k_fold_classification)
            if self.config.progress_bar:
                self.create_classifier_job_and_wait(k_fold_classification)
            content = self.__measure_accuracy(classification, known_categories) 
            self.__write_to_test_output_file(classification['test_output_file'], content)
            
        for known_category in known_categories:
            self.recall[known_category] = round(self.recall[known_category] / classification["k-fold"], 1)
            self.precision[known_category] = round(self.precision[known_category] / classification["k-fold"], 1)
        table = self.__gen_kfold_result_table("dummy", known_categories, True, classification)
        self.__write_to_test_output_file(classification['test_output_file'], f"{table}</body></html>" )
            
    def __do_classification(self, classification):  
        if classification['k-fold']:
            try:
                K = int(classification['k-fold'])
            except:
                raise ConfigError(f'"k-fold" is not an integer: "{K}"')
            if K < 2:
                raise ConfigError(f'"k-fold" cannot be smaller than 2')
            self.__do_kfold_analyses(classification)
        else:
            self.__create_and_train_classifiers(classification)
            if self.config.progress_bar:
                self.create_classifier_job_and_wait(classification) 
            else:
                col_id = self.engine.get_collection_id(self.config.col_name)
                self.engine.create_classifier_job_and_wait(col_id,
                        classification['name'],
                        self.config.col_name,
                        classification['min_score'],
                        classification['num_results'],
                        classification['execution_filters'],
                        classification['output_field']
                     )
                    
    def __get_known_categories(self, training_field):
        return [aggregation["key"] for aggregation in Querier(self.config).search(
            {
                "query"        : "*",
                "aggregations" : [training_field],
                "size"         : 0
            }
        )["aggregations"][training_field]]
        
    def __measure_accuracy(self, classification, known_categories):
        querier = Querier(self.config)
        output_field = classification['output_field']
        training_field = classification['training_field']
        data_set_field = K_FOLD_FIELD
        if len(known_categories) == 0:
            raiseAutoDetectionError(f"No categories found") 
        queries = {}
        all_known_categories = []
        for known_category in known_categories:
            all_known_categories.append(f"{output_field}:{known_category}")
            all_known_categories_ored = " OR ".join(all_known_categories)
        for known_category in known_categories:
            all_other_known_categories = all_known_categories.copy()
            all_other_known_categories.remove(f"{output_field}:{known_category}")
            all_other_known_categories_ored = " OR ".join(all_other_known_categories)
        queries = {}
        for known_category in known_categories:
            queries[known_category] = {}
            queries[known_category]["no_category"] = f"{training_field}:{known_category} AND NOT ({all_known_categories_ored}) AND {data_set_field}:verification"
            for detected_category in known_categories:
                queries[known_category][detected_category] = f"{output_field}:{detected_category} AND {training_field}:{known_category} AND {data_set_field}:verification"
        result = {}
        for known_category in known_categories:
            result[known_category] = {}
            for query in queries[known_category]:
                result[known_category][query] = querier.search({
                    "result": "query_and_numb_of_hits",
                    "query": queries[known_category][query],
                    "size" : 0
                })['meta']['totalDocuments']
        return self.__gen_kfold_result_table(result, known_categories)
        
    def __get_precision(self, category, result):
        r = result[category].copy()
        del r["no_category"]
        try:
            return 100* int(r[category])/int(sum(r.values()))
        except ZeroDivisionError:
            return 100.0
        
    def __get_recall(self, category, result):
        numerator = int(result[category][category]) 
        denominator = sum([item[category] for item in result.values()] + [result[category]["no_category"]])
        try:
            return 100 * numerator / denominator
        except ZeroDivisionError:
            return 100.0
               
    def __gen_kfold_result_table(self, result, known_categories, is_accumulated_score=False, classification=None):
        table = "<table>"
        if is_accumulated_score:
            parameters = f'minScore={classification["min_score"]}, numResults={classification["num_results"]}' 
            table += f'<tr><th colspan="{len(known_categories)+2}" style="text-align:center">RECALL & PRECISION AVERAGE ({parameters})</th></tr>'
        table += "<tr><th></th>"
        for known_category in known_categories:
            table += f"<th>{known_category.upper()}</th>"
        table += f"<th>Precision</th></tr>"
        for known_category in known_categories:
            table += f"<tr><th>DETECTED AS \"{known_category.upper()}\"</th>"
            for detected_category in known_categories:
                if is_accumulated_score:
                    table += f"<td></td>"
                else:
                    table += f"<td>{result[known_category][detected_category]}</td>"
            if is_accumulated_score:
                precision = self.precision[known_category]
            else:
                precision = self.__get_precision(known_category, result)
            table += f"<td>{round(precision, 1)} %</td></tr>"
            if not is_accumulated_score:
                self.precision[known_category] += precision
        table += f"<tr><th>NOT DETECTED AS ANYTHING</th>"
        for known_category in known_categories:
            if is_accumulated_score:
                table += f"<td></td>"
            else:
                table += f"<td>{result[known_category]['no_category']}</td>"
        table += "</tr><tr><th>Recall</th>"
        for known_category in known_categories:
            if is_accumulated_score:
                recall = self.recall[known_category]
            else:
                recall = self.__get_recall(known_category, result)
            table += f"<td>{round(recall, 1)} %</td>" 
            if not is_accumulated_score:
                self.recall[known_category] += recall
        table += "</tr></table><br>" 
        return table

 
class Sampler(EngineConnector):

    def create_sampling_job_and_wait(self):
        if self.config.sampling:
            col_id = self.engine.get_collection_id(self.config.col_name)
            job_id = self.engine.start_sampling(col_id, self.config.sampling)
            elapsed_time = JobsHandler(self.config).track_job(job_id, "SAMPLING")
            job_report = self.engine.get_job(job_id)
            if not "sample" in job_report:
                raise DataFormatError('No "sample" item in job output')
            if self.config.sampling_report_time:
                self._print(f"The sampling operation took {str(elapsed_time)} seconds")
            return [doc["id"] for doc in job_report["sample"]]
        else:
            log.info('No sampling job')
 
 
class Clusterer(EngineConnector):

    def create_clustering_job_and_wait(self):
        if self.config.clustering:
            col_id = self.engine.get_collection_id(self.config.col_name)
            config = {
                "collectionId" : col_id,
                "type" : "CLUSTERING",
                "filters" : self.config.clustering_filters,
                "outputField" : self.config.clustering_output_field
            }
            if "dummy" in self.config.clustering:
                del self.config.clustering["dummy"]
            if self.config.clustering:
                config["settings"] = {"clustering": self.config.clustering}
            if self.config.progress_bar:
                return JobsHandler(self.config).job_processesing("CLUSTERING", config)
            else:
                self.engine.process_collection_and_wait(col_id)
        else:
            log.info('No clustering job')

            
class Querier(EngineConnector):

    def search(self, search):
        col_id = self.engine.get_collection_id(self.config.col_name)
        return self.engine.meta_search(col_id, search)
 
    def get_search_result_pages(self, search):
        col_id = self.engine.get_collection_id(self.config.col_name)
        result = self.engine.search_collection(col_id, search["query"], MAX_PAGE_SIZE, 0, search["filters"], search["exclude"], search["aggregations"], scroll=True, meta_data=True)
        docs = result["result"]
        while True:
            if len(docs):
                yield docs
            if len(docs) < MAX_PAGE_SIZE:
                break
            try:
                link = result["_links"]["next"]["href"]
            except:
                break
            result = self.engine.next_scroll_page(link)
            docs = result["result"]
       
    def get_collection_schema(self):
        col_id = self.engine.get_collection_id(self.config.col_name)
        return self.engine.get_collection_schema(col_id)
        
        
class Reporter(EngineConnector): 

    def __get_jobs_status(self):
        jobs_handler = JobsHandler(self.config)
        col_id = None
        job_id = None
        latest_job_only = False
        if self.config.report_jobs in ["collection", "collection_latest"]:
            col_id = self.engine.get_collection_id(self.config.col_name)
            if self.config.report_jobs == "collection_latest":
                latest_job_only = True
        elif type(self.config.report_jobs) is int:
            job_id = self.config.report_jobs
        elif self.config.report_jobs != "all":
            raise ValueError(f"Unknown jobs value '{self.config.report_jobs}'")
        main_job_only = True
        jobs_handler.print_jobs_overview(None, col_id, job_id, latest_job_only, self.config.report_verbose)
                 
    def __get_logs_status(self):
        if self.config.report_logs_source and self.config.report_retrieve_logs:
            raise ValueError("Reporting options 'logs_source' and 'retrieve_logs' cannot both be set")
        if self.config.report_logs_source:
            if self.config.report_logs_source.startswith('http'):
                FileHandlingTools().recreate_directory(DOWNLOAD_DIR)
                log_file_path = WebContent().download(self.config.report_logs_source, DOWNLOAD_DIR)
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
        LogAnalyzer(
            log_file_path, self.config.report_output_destination, TMP_LOG_UNPACK_DIR, 
            self.config.report_dump_lines, self.config.report_custom_regex, 
            self.config.report_noise_filtering).analyze()

    def __get_memory_config(self):
        print ("got here")

    def do_reporting(self):
        if self.config.report_jobs:
            self.__get_jobs_status()
        if self.config.report_logs_source or self.config.report_retrieve_logs:
            self.__get_logs_status()
      

class DocumentUpdater(EngineConnector):

    def __update_documents_by_query(self, docs_update):
        col_id = self.engine.get_collection_id(self.config.col_name)
        if self.config.progress_bar:
            if not "values" in docs_update:
                docs_update["values"] = None
            job_id = self.engine.update_documents_by_query(
                          col_id, docs_update["query"], docs_update["filters"], 
                          docs_update["action"], docs_update["field"], docs_update["values"])
            return JobsHandler(self.config).track_job(job_id, "UPDATE")
        else:
            return self.engine.update_documents_by_query_and_wait(
                          col_id, docs_update["query"], docs_update["filters"], 
                          docs_update["action"], docs_update["field"], docs_update["values"])
            
    def __get_doc_ids(self, query, limit=None, random_order=False):
        col_id = self.engine.get_collection_id(self.config.col_name)
        return self.engine.get_doc_ids(col_id, query, limit, random_order)
        
    def do_updates(self, docs_updates=None):
        if not docs_updates:
            docs_updates = self.config.docs_updates
        for docs_update in docs_updates:
            if not "report_time" in docs_update:
                docs_update["report_time"] = False
            if not "values" in docs_update:
                docs_update["values"] = []
            if not "filters" in docs_update:
                docs_update["filters"] = []
            filters = docs_update["filters"]
            if docs_update["filters"] == "DOC_IDS":
                query = docs_update["id_extraction_query"]
                limit = docs_update["numb_of_docs_to_update"]
                filters = [{"field":"_id", "value": self.__get_doc_ids(query, limit)}]
            docs_update["filters"] = filters
            elapsed_time = self.__update_documents_by_query(docs_update)
            if docs_update["report_time"]:
                self._print(f"The documents update operation took {str(elapsed_time)} seconds")

            
class LogAnalyzer():

    def __init__(self, log_file_path, output_destination, log_unpacking_dir, dump_lines=False, custom_regex=None, noise_filtering=False):
        self.output_destination = output_destination
        self.log_file = log_file_path
        self.dump_lines = dump_lines
        self.custom_regex = custom_regex
        self.noise_filtering = noise_filtering
        self.log_unpacking_dir = log_unpacking_dir
        self.unzip_dir = UNZIP_DIR
        if not exists(log_file_path):
            raise ConfigError(f"There is no {log_file_path}")
        self.filtered_patterns = [
            "^.*RestControllerRequestLoggingAspect: triggering JobController.getJob\(\.\.\) with args:.*$"
        ] 
        self.error_info = [
            {
                "pattern" : r"^.*(20[12][0-9]-[01][0-9]-[0-9][0-9]T[012][0-9]:[0-6][0-9]:[0-9][0-9])\.[0-9]{9,9}Z .*$",
                "extraction": [("Log start time", 1)],
                "occurence": "first"
            },
            {
                "pattern" : r"^.*(20[12][0-9]-[01][0-9]-[0-9][0-9]T[012][0-9]:[0-6][0-9]:[0-9][0-9])\.[0-9]{9,9}Z .*$",
                "extraction": [("Log end time", 1)],
                "occurence": "last",
                "key": "end_date"
            },
            {
                "pattern" : r"^.*free: (.*gb)\[(.*%)\].*$",
                "extraction": [("At the start: Available disk space: ", 1), ("Available disk capacity: ", 2)],
                "occurence": "first"
            }, 
            {
                "pattern" : r"^.*free: (.*gb)\[(.*%)\].*$",
                "extraction": [("At the end: Available disk space: ", 1), ("Available disk capacity: ", 2)],
                "occurence": "last",
                "key": "disk_space"
            },
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
            },
            {
                "pattern" : r"^.*main platform.config.AyfieVersion: (.*)$",   
                "extraction": [("ayfie Inspector", 1)] 
            },
            {
                "pattern" : r"^.*initializing general extractor with name _gdpr.*$",
                "extraction": [("GDPR", "Enabled")]
            },
            {
                "pattern" : r"^.*skipExtractors=\[_gdpr\].*$",
                "extraction": [("GDPR", "Disabled")]
            }
        ]
        self.symptoms = [
            {
                "pattern" : r"^.*insufficient memory for the Java Runtime Environment.*$",
                "indication": "that the host has run out of available RAM (configured via .env or actual)"
            },
            {
                "pattern" : r"^.*Ignoring unknown properties with keys.*$",
                "indication": "one has used an API parameter that does not exist or is mispelled."
            },
            {
                "pattern" : r"^.*All masters are unresponsive.*$",
                "indication": "there was a insufficient process clean up after a failure. Corrective action would be to bounce the api container."
            },
            {
                "pattern" : r"^.*Missing an output location for shuffle.*$",
                "indication": "the system has run out of memory"
            },
            {
                "pattern" : r"^.*None of the configured nodes (were|are) available.*$",
                "indication": "the host is low on disk and/or memory."
            },
            {
                "pattern" : r"^.*java\.lang\.OutOfMemoryError.*$",
                "indication": "the JVM has run out of memory."
            },            
            {
                "pattern" : r"^.*all nodes failed.*$",
                "indication": "we have come across what before used to be referred to as 'slow disk' errors, but that we now know are more nuanced than that (will be updated)"
            },
            {
                "pattern" : r"^.*no other nodes left.*$",
                "indication": "we have come across what before used to be referred to as 'slow disk' errors, but that we now know are more nuanced than that (will be updated)"
            },
            {
                "pattern" : r"^.*all shards failed.*$",
                "indication": "something is off with the Elasticsearch index partitions. Check for a possilbe scroll request on the previous log line as scroll requests that comes more than 20 minutes (the fixed search result scroll timeout value) apart could cause this issue"
            },
            {
                "pattern" : r"^.*index read-only / allow delete \(api\).*$",
                "indication": "the system is low on disk and Elasticsearch has write protecting the index to prevent possible index corruption."
            },
            {
                "pattern" : r"^.*low disk watermark \[[0-9]+%\] exceeded.*$",
                "indication": "the system is starting to get low on disk and Elasticsearch is taking its first precautionary meassures to prevent future index corruption."
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
                "indication": "the Chinese tokenizer has an expired license (fixed in 1.18.3)"
            },            
            {
                "pattern" : r"^.*createJob.*(gramianSingularVectors|minLikelihood|gramianThreshold|minSimilarity|preSampling).*$",
                "indication": "that parameters that are better left unchanged are set."
            },
            {
                "pattern" : r"^.*Update by query job failed after updating.*$",
                "indication": "one is doing document tagging while some other job (for instance classification) is ongoing."
            },
            {
                "pattern" : r"^.*Classifier output field is expected to be a list.*$",
                "indication": "the classifier output field needs to be a list, but is not."
            },
            {
                "pattern" : r"^.*all indices on this node will be marked read-only.*$",
                "indication": "the system is low on disk and Elasticsearch has write protecting the index to prevent possible index corruption."
            },
            {
                "pattern" : r"^.*java.io.IOException: No space left on device.*$",
                "indication": "that one is out of disk space."
            },
            {
                "pattern" : r"^.*flood stage disk watermark [[0-9]{1,20%] exceeded on.*$",
                "indication": "that one is out of disk space."
            },
            {
                "pattern" : r"^.*(jobState=FAILED|,description=Execution failed: ).*$",
                "indication": "a job has failed. Use the given jobId to look up failure details with 'curl server:port/ayfie/v1/jobs/jobId' (replace `jobId` with the id)"
            },
            {
                "pattern" : r"^.*hs_err_pid[0-9]+\.log.*$",
                "indication": "there is a JVM process crash report at the given location within the given container."
            },
            {
                "pattern" : r"^.*Total size of serialized results.+bigger than spark.driver.maxResultSize.*$",
                "indication": "one has run  into a hard coded mem limit that has been removed in later versions."
            }
        ] 
        if self.custom_regex:
            self.symptoms.append({
                "pattern" : self.custom_regex,
                "indication": "your custom regEx produced result"
            })
            
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
            
    def __line_filtering(self, line):
        if len(line.strip()) == 0:
            return ""
        for pattern in self.filtered_patterns:
            m = match(pattern, line)
            if m:
                return ""
        return line
        
    def __process_log_file(self, log_file):
        if not exists(log_file):
            raise ValueError(f"Path '{log_file}' does not exists")
        if isdir(log_file):
            raise ValueError(f"'{log_file}' is not a file, but a directory")
        file_type, detected_encoding = FileHandlingTools().get_file_type(log_file) 
        encoding = "utf-8"
        if detected_encoding:
            if detected_encoding == "utf_16_le":
                encoding = "utf-16"
            else:
                encoding = detected_encoding
        error_info = deepcopy(self.error_info)
        with open(log_file, "r", encoding=encoding) as f:
            if self.noise_filtering:
                output_file = log_file + ".filtered"
            else:
                output_file = "dummy.txt"
            with open(output_file, "a") as filtered_file:
                self.line_count = 0
                self.info_pieces = {}
                try:
                    for line in f:
                        if self.noise_filtering:
                            filtered_file.write(self.__line_filtering(line))
                        if self.line_count == 0:
                            if not line.strip().startswith("Attaching to"):
                                raise DataFormatError(f"File '{log_file}' is not recognized as a ayfie Inspector log file")
                        self.line_count += 1
                        for info in error_info:                    
                            if "occurence" in info and info["occurence"] == "completed":
                                continue
                            m = match(info["pattern"], line)
                            if m:
                                output_line = []
                                for item in info["extraction"]:
                                    item_name = item[0]
                                    item_value = item[1]
                                    if type(item_name) is int:
                                        item_name = m.group(item_name)
                                    if type(item_value) is int:
                                        item_value = m.group(item_value)
                                    output_line.append(f"{item_name}: {item_value}")
                                if "key" in info:
                                    self.info_pieces["key"] = ", ".join(output_line)
                                else:
                                    self.info_pieces[", ".join(output_line)] = False
                                if "occurence" in info and info["occurence"] == "first":
                                    info["occurence"] = "completed"                
                        for symptom in self.symptoms:
                            m = match(symptom["pattern"], line)
                            if m:
                                self.errors_detected = True
                                symptom["occurences"] += 1
                                symptom["last_occurence_line"] = self.line_count
                                symptom["last_occurence_sample"] = m.group(0)
                                if self.dump_lines:
                                    print(m.group(0))
                        m = match(r"Exception", line)
                        if m:
                            if not m.group(0) in self.exceptions:
                                self.exceptions[m.group(0)] = 0
                            self.exceptions[m.group(0)] += 1
                except UnicodeDecodeError as e:
                    raise DataFormatError(f"'File {log_file}' is most likley a binary file (or at a mininimum of wrong encoding)")
                except Exception:
                    raise
                
    def produce_output(self, title, data_dict):
        output = ""
        if len(data_dict) > 0:
            output = f"\n====== {title} ======\n"
            for key in data_dict.keys():
                if data_dict[key]:
                    output += f"{data_dict[key]}\n"
                else:
                    output += f"{key}\n"
        return output
         
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
                if self.line_count == 0:
                    is_log_file = False
            except DataFormatError:
                is_log_file = False
            analyses_output += f'\n\n====== FILE "{log_file}" {line_info}======\n'
            if is_log_file:
                analyses_output += self.produce_output("Time & System Information", self.info_pieces)
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

                    
class JobsHandler(EngineConnector):
    
    def __start_job(self, job_type, settings, more_params={}):
        config = {
            "collectionId" : self.engine.get_collection_id(self.config.col_name),
            "type" : job_type
        }
        if settings:
            config["settings"] = settings
        for key, value in more_params.items():
            config[key] = value
        return self.engine.create_job(config)
        
    def track_job(self, job_id, job_type):
        elapsed_time = -1
        completed_sub_job_ids = []
        start_time = None
        job = {
            "state" : self.__get_job_status(job_id),
            "id": job_id,
            "type": job_type
        }
        tick = 0
        self.__job_reporting_output_line(job, 'JOB', "\r", tick)
        while True:
            tick += 1
            all_job_statuses = self.__get_job_and_sub_job_status_obj(job_id, completed_sub_job_ids)
            if not start_time:          
                if all_job_statuses["job"]['state'] in ['RUNNING','SUCCEEDED','FAILED']:
                    start_time = time()
            if start_time:
                elapsed_time = int(time() - start_time)
            for sub_job in all_job_statuses["sub_jobs"]:
                end = "\r"
                if not sub_job["state"] in ["SCHEDULED", "RUNNING"]:
                    completed_sub_job_ids += [sub_job["id"] for sub_job in all_job_statuses["sub_jobs"]]
                    end = "\n"
                if not sub_job["state"] in ["SCHEDULED"]:
                    self.__job_reporting_output_line(sub_job, 'sub-job', end, tick)
            job = all_job_statuses["job"]
            if job['state'] in ["SUCCEEDED", "FAILED"]:
                self.__job_reporting_output_line(job, 'JOB')
                tick = 0
                break
            sleep(JOB_POLLING_INTERVAL)
        return elapsed_time

    def job_processesing(self, job_type, settings, more_params={}):
        job_id = self.__start_job(job_type, settings, more_params)
        self.track_job(job_id, job_type)
        
    def __job_reporting_output_line(self, job, prefix, end="\n", tick=None):
        rotator = " "
        """
        if tick:
            position = tick % 8
            if position in [0, 4]:
                rotator = "|"
            elif position in [1, 5]:
                rotator = "/"
            elif position in [2, 6]:
                rotator = "—"
            elif position in [3, 7]:
                rotator = "\\"
        """
        self._print(f'{rotator} {prefix.rjust(8)}: {job["state"].ljust(10)} {job["id"].ljust(10)} {job["type"].ljust(22)}', end)

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
        
    def __update_structures(self, job):
        job_info = self.__extract_job_info(job)
        if job_info["type"] == "PROCESSING":
            if not "collectionId" in job_info:
                return
            collection_id = job_info["collectionId"]   
            if not collection_id in self.job_by_collection_id:
                self.job_by_collection_id[collection_id] = []
            self.job_by_collection_id[collection_id].append(job_info)
        self.job_and_sub_job_by_id[job_info["id"]] = job_info
        
    def __gen_job_lookup_dicts(self, json_job_list):
        self.job_and_sub_job_by_id = {}
        self.job_by_collection_id = {}
        job_list = loads(json_job_list)
        if (not "_embedded" in job_list) or (not "jobinstances" in job_list["_embedded"]):
            return {}, {}
        for job in job_list["_embedded"]["jobinstances"]:
            self.__update_structures(job)
                    
    def __get_job_and_sub_jobs_structure(self):
        job_and_sub_jobs_structure = {}
        for collection_id in self.job_by_collection_id.keys():
            self.job_by_collection_id[collection_id] = sorted(self.job_by_collection_id[collection_id], key=lambda job_info: job_info["time_stamp"])
        for collection_id in self.job_by_collection_id.keys():
            job_and_sub_jobs_structure[collection_id] = {}
            for job in self.job_by_collection_id[collection_id]:
                job_and_sub_jobs_structure[collection_id][job["id"]] = []
                for sub_job_id in job["sub_jobs"]:
                    job_and_sub_jobs_structure[collection_id][job["id"]].append(sub_job_id)
        for collection_id in self.job_by_collection_id.keys():
            for job in self.job_by_collection_id[collection_id]:
                job['sub_jobs'] = self.__get_ordered_sub_job_list(job)
        return job_and_sub_jobs_structure
                
    def __gen_auxilary_data_structures(self, json_job_list):
        self.__gen_job_lookup_dicts(json_job_list)
        self.job_and_sub_jobs_structure = self.__get_job_and_sub_jobs_structure()
    
    def __get_job(self, job_id):
        if not job_id in self.job_and_sub_job_by_id:
            job = self.engine.get_job(job_id)
            self.__update_structures(job)
        return self.job_and_sub_job_by_id[job_id]
           
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
                          
    def __get_job_and_sub_job_status_obj(self, job_id, completed_sub_jobs):
        json_job_list = dumps(self.engine.get_jobs())
        self.__gen_auxilary_data_structures(json_job_list)
        sub_jobs = self.__get_list_of_sub_jobs(json_job_list, job_id, completed_sub_jobs)
        return {"job": self.__get_job(job_id), "sub_jobs": sub_jobs}
        
    def __get_job_status(self, job_id):
        json_job_list = dumps(self.engine.get_jobs())
        self.__gen_auxilary_data_structures(json_job_list)
        return(self.__get_job(job_id)['state']) 
        
    def __print_job_and_sub_jobs(self, job_info, verbose=False):
        if verbose:
            print(dumps(self.engine.get_job(job_info["id"]), indent=3))
        else:
            print(f'   -job: {job_info["state"]} {job_info["id"]} {job_info["type"]} {job_info["time_stamp"].replace("T", " ").split(".")[0]}')
        for sub_job_id in job_info['sub_jobs']:
            if verbose:
                print(dumps(self.engine.get_job(sub_job_id), indent=3))
            else:
                sub_job_info = self.__get_job(sub_job_id)
                print(f'       -sub-job: {sub_job_info["state"].ljust(10)} {sub_job_id.ljust(10)} {sub_job_info["type"].ljust(22)}  {sub_job_info["clock_time"].ljust(8)}')

    def wait_for_job_to_finish(self, job_id, timeout=None):
        start_time = None
        while True:
            job_status = self.__get_job_status(job_id)
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
            json_job_list = dumps(self.engine.get_jobs()) 
        self.__gen_auxilary_data_structures(json_job_list)
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
                    self.__print_job_and_sub_jobs(job_info, verbose)
            if latest_job_only:
                self.__print_job_and_sub_jobs(self.__get_job(latest_job_id), verbose)

                
class Config():

    def __init__(self, config):
        self.engine           = self.__get_item(config, 'engine', INSPECTOR)
        self.silent_mode      = self.__get_item(config, 'silent_mode', True)
        self.config_source    = self.__get_item(config, 'config_source', None)
        self.server           = self.__get_item(config, 'server', '127.0.0.1')
        self.port             = self.__get_item(config, 'port', '80')
        self.api_version      = self.__get_item(config, 'api_version', 'v1')
        self.col_name         = self.__get_item(config, 'col_name', None)
        self.csv_mappings     = self.__get_item(config, 'csv_mappings', None)
        self.progress_bar     = self.__get_item(config, 'progress_bar', True)
        self.document_update  = self.__get_item(config, 'document_update', False)
        self.doc_retrieval    = self.__get_item(config, 'document_retrieval', False)
        self.__init_doc_retrieval(self.doc_retrieval)
        self.processing       = self.__get_item(config, 'processing', False)
        self.__init_processing(self.processing)
        self.sampling         = self.__get_item(config, 'sampling', False)        
        self.__init_sampling(self.sampling)
        self.api_call         = self.__get_item(config, 'api_call', False)
        self.__init_api_call(self.api_call)
        self.schema           = self.__get_item(config, 'schema', False)
        self.__init_schema(self.schema)
        self.feeding          = self.__get_item(config, 'feeding', False)
        self.__init_feeder(self.feeding)
        self.clustering       = self.__get_item(config, 'clustering', False)
        self.__init_clustering(self.clustering)
        self.classifications  = self.__get_item(config, 'classification', False)
        self.__init_classification(self.classifications)
        self.documents_updates= self.__get_item(config, 'documents_update', False)
        self.__init_documents_updates(self.documents_updates)
        self.searches         = self.__get_item(config, 'search', False)
        self.__init_search(self.searches)
        self.reporting        = self.__get_item(config, 'reporting', False)
        self.__init_report(self.reporting)
        self.regression_testing = self.__get_item(config, 'regression_testing', False)
        self.__init_regression_testing(self.regression_testing)
        self.__inputDataValidation()

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
        
    def __init_api_call(self, api_call):
        self.api_call_endpoint        = self.__get_item(api_call, 'endpoint', None)
        self.api_call_http_verb       = self.__get_item(api_call, 'http_verb', "GET")
        self.api_call_os              = self.__get_item(api_call, 'os', "linux")
        self.api_call_tool            = self.__get_item(api_call, 'tool', "curl")
        self.api_call_indented        = self.__get_item(api_call, 'indented', False)
        
    def __init_schema(self, schema_changes):
        if not schema_changes:
            self.schema_changes = None
            return 
        if not type(schema_changes) is list:
            schema_changes = [schema_changes]  
        self.schema_changes = []
        for schema_change in schema_changes:
            self.schema_changes.append({
                "name"       : self.__get_item(schema_change, 'name', None),
                "type"       : self.__get_item(schema_change, 'type', "TEXT"),
                "list"       : self.__get_item(schema_change, 'list', False),
                "roles"      : self.__get_item(schema_change, 'roles', []),
                "properties" : self.__get_item(schema_change, 'properties', []),
            })
        if len([1 for change in self.schema_changes if not "name" in change]):
            raise ConfigError('Schema configuration requires a field name to be set')
    
    def __init_feeder(self, feeding):
        self.data_source              = self.__get_item(feeding, 'data_source', None)
        if self.data_source and not isabs(self.data_source):
            self.data_source = join(path_split(self.config_source)[0], self.data_source)
        self.csv_mappings             = self.__get_item(feeding, 'csv_mappings', self.csv_mappings)
        self.data_type                = self.__get_item(feeding, 'data_type', AUTO)
        self.fallback_data_type       = self.__get_item(feeding, 'fallback_data_type', None)
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
        self.feeding_report_time      = self.__get_item(feeding, 'report_time', False)
        self.report_doc_error         = self.__get_item(feeding, 'report_doc_error', False)
        self.no_processing            = self.__get_item(feeding, 'no_processing', False)
        self.email_address_separator  = self.__get_item(feeding, 'email_address_separator', None)
        self.ignore_pdfs              = self.__get_item(feeding, 'ignore_pdfs', True)
        self.treat_csv_as_text        = self.__get_item(feeding, 'treat_csv_as_text', False)
        self.max_docs_to_feed         = self.__get_item(feeding, 'max_docs_to_feed', None)
        #self.id_from_filename_regex   = self.__get_item(feeding, 'id_from_filename_regex', None)
        self.doc_fragmentation        = self.__get_item(feeding, 'doc_fragmentation', False)
        self.doc_fragment_length      = self.__get_item(feeding, 'doc_fragment_length', 100)   
        self.max_doc_fragments        = self.__get_item(feeding, 'max_doc_fragments', 10000) 
        self.fragmented_doc_dir       = self.__get_item(feeding, 'fragmented_doc_dir', None) 
        self.convert_multipart_msg    = self.__get_item(feeding, 'convert_multipart_msg', False) 
        self.flag_multipart_msg       = self.__get_item(feeding, 'flag_multipart_msg', False)         
        id_conv_table_file            = self.__get_item(feeding, 'id_conversion_table_file', None)
        id_conv_table_delimiter       = self.__get_item(feeding, 'id_conversion_table_delimiter', ",")
        id_conv_table_id_column       = self.__get_item(feeding, 'id_conv_table_id_column', None)
        id_conv_table_filename_column = self.__get_item(feeding, 'id_conv_table_filename_column', None)         
        self.id_mappings = {}
        if id_conv_table_file:
            minimum_columns = max(id_conv_table_id_column, id_conv_table_filename_column) + 1 
            with open(id_conv_table_file) as f:
                for line in f.read().split('\n'):
                    id_mapping = line.split(id_conv_table_delimiter)
                    if len(id_mapping) >= minimum_columns:
                        self.id_mappings[id_mapping[id_conv_table_filename_column]] = id_mapping[id_conv_table_id_column]
        # make a function of this to combine with almost identical code below              
        if type(self.id_picking_list) == str:
            filename = self.id_picking_list
            try:
                self.id_picking_list = loads(open(filename, "r").read())
            except JSONDecodeError:
                raise  DataFormatError(f"Bad JSON in file '{filename}' holder doc id list")
            except FileNotFoundError:
                raise ValueError(f"File '{filename}' with doc id list does not exists")
        
    def __init_processing(self, processing): 
        self.thread_min_chunks_overlap= self.__get_item(processing, 'thread_min_chunks_overlap', None)
        self.near_duplicate_evaluation= self.__get_item(processing, 'near_duplicate_evaluation', None)
        self.processing_report_time   = self.__get_item(processing, 'report_time', False)
        
    def __init_sampling(self, sampling):
        if sampling:
            self.sampling = {
                "method"                    : self.__get_item(sampling, 'method', "SMART"),
                "size"                      : self.__get_item(sampling, 'size', 100),
                "smart_specificity"         : self.__get_item(sampling, 'smartSpecificity', 0.3),
                "smart_dimensions"          : self.__get_item(sampling, 'smartDimensions', 100),
                "smart_min_df"              : self.__get_item(sampling, 'smartMinDf', 0.0005),
                "smart_presampling"         : self.__get_item(sampling, 'smartPresampling', 100),
                "filters"                   : self.__get_item(sampling, 'filters', []), 
            }
            self.sampling_report_time       = self.__get_item(sampling, 'report_time', False)

    def __init_clustering(self, clustering):
        if not clustering:
            return
        c = {}
        c['dummy']                    = True
        c['clustersPerLevel']         = self.__get_item(clustering, 'clustersPerLevel', None)
        c['documentsPerCluster']      = self.__get_item(clustering, 'documentsPerCluster', None)
        c['maxRecursionDepth']        = self.__get_item(clustering, 'maxRecursionDepth', None)
        c['minDf']                    = self.__get_item(clustering, 'minDf', None)
        c['maxDfFraction']            = self.__get_item(clustering, 'maxDfFraction', None)
        c = {key: c[key] for key in c if c[key] != None}
        self.clustering               = c
        self.clustering_filters       = self.__get_item(clustering, 'filters', [])
        self.clustering_output_field  = self.__get_item(clustering, 'outputField', '_cluster')
        self.clustering_report_time   = self.__get_item(clustering, 'report_time', False)
        
    def __init_classification(self, classifications):
        if not classifications:
            return 
        self.classifications = []
        if not type(classifications) is list:
            classifications = [classifications]
        for classification in classifications:
            self.classifications.append({
                "name"              : self.__get_item(classification, 'name', None),
                "training_field"    : self.__get_item(classification, 'trainingClassesField', 'trainingClass'),
                "min_score"         : self.__get_item(classification, 'minScore', 0.6),
                "num_results"       : self.__get_item(classification, 'numResults', 1),
                "training_filters"  : self.__get_item(classification, 'training_filters', []),
                "execution_filters" : self.__get_item(classification, 'execution_filters', []),
                "output_field"      : self.__get_item(classification, 'outputField', 'pathclassification'),
                "k-fold"            : self.__get_item(classification, 'k-fold', None),
                "test_output_file"  : self.__get_item(classification, 'test_output_file', "test_output_file.html")
            })
        
    def __init_documents_updates(self, documents_updates):
        self.docs_updates = []
        if not type(documents_updates) is list:
            documents_updates = [documents_updates]
        for documents_update in documents_updates:
            self.docs_updates.append({
                "query"                  : self.__get_item(documents_update, 'query', "*"),
                "action"                 : self.__get_item(documents_update, 'action', ADD_VALUES),
                "field"                  : self.__get_item(documents_update, 'field', 'trainingClass'),
                "values"                 : self.__get_item(documents_update, 'values', []),
                "filters"                : self.__get_item(documents_update, 'filters', []),
                "id_extraction_query"    : self.__get_item(documents_update, 'id_extraction_query', "*"),
                "numb_of_docs_to_update" : self.__get_item(documents_update, 'numb_of_docs', "ALL"),
                "report_time"            : self.__get_item(documents_update, 'report_time', False)
            })
        for docs_update in self.docs_updates:
            if docs_update['filters'] != "DOC_IDS":
                for filter in docs_update['filters']:
                    if filter['field'] == "_id" and type(filter['value']) == str:
                        filename = filter['value']
                        try:
                            filter['value'] = loads(open(filename, "r").read())
                        except JSONDecodeError:
                            raise  DataFormatError(f"Bad JSON in file '{filename}' holder doc id list")
                        except FileNotFoundError:
                            raise ValueError(f"File '{filename}' with doc id list does not exists")
                            
    def __init_doc_retrieval(self, doc_retrieval):
        self.doc_retrieval_output_file            = self.__get_item(doc_retrieval, 'output_file', "output.csv")
        self.doc_retrieval_fields                 = self.__get_item(doc_retrieval, 'fields', None)
        self.doc_retrieval_batch_size             = self.__get_item(doc_retrieval, 'batch_size', 1000)
        self.doc_retrieval_csv_dialect            = self.__get_item(doc_retrieval, 'csv_dialect', None)
        self.doc_retrieval_separator              = self.__get_item(doc_retrieval, 'csv_separator', "|")
        self.doc_retrieval_query                  = self.__get_item(doc_retrieval, 'query', "")
        self.doc_retrieval_filters                = self.__get_item(doc_retrieval, 'filters', [])
        self.doc_retrieval_exclude                = self.__get_item(doc_retrieval, 'exclude', [])
        self.doc_retrieval_max_docs               = self.__get_item(doc_retrieval, 'max_docs', None)
        self.doc_retrieval_csv_header_row         = self.__get_item(doc_retrieval, 'csv_header_row', True) 
        self.doc_retrieval_report_frequency       = self.__get_item(doc_retrieval, 'report_frequency', 1000)  
        self.doc_retrieval_drop_underscore_fields = self.__get_item(doc_retrieval, 'drop_underscore_fields', True) 
        if self.doc_retrieval_csv_dialect:
            if not self.doc_retrieval_csv_dialect in csv.list_dialects():
                raise ValueError(f"'{self.doc_retrieval_}' is not csv dialect")
        else:
            self.doc_retrieval_csv_dialect = 'custom_config'
            csv.register_dialect(
                self.doc_retrieval_csv_dialect, 
                delimiter        = self.__get_item(doc_retrieval, 'csv_delimiter', ','),
                doublequote      = self.__get_item(doc_retrieval, 'csv_double_quote', True), 
                quotechar        = self.__get_item(doc_retrieval, 'csv_quote_char', '"'), 
                lineterminator   = self.__get_item(doc_retrieval, 'csv_line_terminator', '\r\n'), 
                escapechar       = self.__get_item(doc_retrieval, 'csv_escape_char', None),
                quoting          = eval("csv." + self.__get_item(doc_retrieval, 'csv_quoting', "QUOTE_MINIMAL")),
                skipinitialspace = self.__get_item(doc_retrieval, 'csv_skip_initial_space', False)
            )

    def __init_search(self, searches):
        if not searches:
            self.searches = []
            return  
        self.searches = []
        if not type(searches) is list:
            searches = [searches]
        for search in searches:
            self.searches.append({
                "query_file"         : self.__get_item(search, 'query_file', None),
                "result"             : self.__get_item(search, 'result', "display"),
                "classifier_field"   : self.__get_item(search, 'classifier_field', None),
                "highlight"          : self.__get_item(search, 'highlight', True),
                "sort_criterion"     : self.__get_item(search, 'sort_criterion', "_score"),
                "sort_order"         : self.__get_item(search, 'sort_order', "desc"),
                "minScore"           : self.__get_item(search, 'minScore', 0.0),
                "exclude"            : self.__get_item(search, 'exclude', []),
                "aggregations"       : self.__get_item(search, 'aggregations', []),
                "filters"            : self.__get_item(search, 'filters', []),
                "query"              : self.__get_item(search, 'query', "*"),
                "scroll"             : self.__get_item(search, 'scroll', False),
                "size"               : self.__get_item(search, 'size', 10),
                "offset"             : self.__get_item(search, 'offset', 0),
            })

    def __init_report(self, report):
        self.report_output_destination= self.__get_item(report, 'output_destination', "terminal")
        self.report_verbose           = self.__get_item(report, 'verbose', False)
        self.report_jobs              = self.__get_item(report, 'jobs', False)
        self.report_logs_source       = self.__get_item(report, 'logs_source', False)
        self.report_noise_filtering   = self.__get_item(report, 'noise_filtering', False)
        self.report_retrieve_logs     = self.__get_item(report, 'retrieve_logs', False)
        self.report_dump_lines        = self.__get_item(report, 'dump_lines', False)
        self.report_custom_regex      = self.__get_item(report, 'custom_regex', None)
        self.report_jobs_overview     = self.__get_item(report, 'jobs_overview', False)
        
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
        if self.progress_bar and self.engine == SOLR:
            self.progress_bar = False

            
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

    def process_test_result(self, search, result):
        if not self.config.config_source:
            self.config.config_source = "The test"
        if type(search["result"]) is bool:
            if search["result"]:
                print(f'CORRECT: {self.config.config_source} is hardcoded to be correct')
            else:
                print(f'FAILURE: {self.config.config_source} is hardcoded to be a failure')
        elif type(search["result"]) is int:
            expected_results = int(search["result"])
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
            if self.config.engine == INSPECTOR:
                data_source = DataSource(self.config)
            elif self.config.engine == SOLR:
                data_source = SolrDataSource(self.config)
        self.schema_manager = SchemaManager(self.config)
        self.feeder = Feeder(self.config, data_source)
        self.querier = Querier(self.config)
        self.document_updater = DocumentUpdater(self.config)
        self.reporter = Reporter(self.config)
        self.clusterer  = Clusterer(self.config)
        self.classifier = Classifier(self.config)
        self.doc_retriever = DocumentRetriever(self.config)
        self.sampler = Sampler(self.config)
        
    def run_config(self):
        if self.config.api_call:
            raise NotImplementedError("ApiCallGenerator yet to be implemented")
            print(ApiCallGenerator(self.config).generate())
            return 
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
                    if self.config.schema_changes:
                        self.schema_manager.add_field_if_absent()
                    if self.config.no_processing:
                        self.feeder.feed_documents_and_commit()
                    else:
                        self.feeder.feed_documents_commit_and_process()
                else:
                    raise ConfigError(f'Cannot feed to non-existing collection "{self.config.col_name}"')
            else:
                if not self.feeder.collection_exists():
                    raise ConfigError(f'There is no collection "{self.config.col_name}"')
                    
            if self.config.processing:
                self.feeder.process()
            if self.config.schema_changes:
                self.schema_manager.add_field_if_absent()
            if self.config.documents_updates:
                self.document_updater.do_updates()
            if self.config.clustering:
                self.clusterer.create_clustering_job_and_wait()
            if self.config.classifications:
                self.classifier.do_classifications()
            if self.config.doc_retrieval:
                self.doc_retriever.retrieve_documents()
            if self.config.sampling:
                pretty_print(self.sampler.create_sampling_job_and_wait())
            for search in self.config.searches:
                try:
                    result = self.querier.search(search)
                except FileNotFoundError as e:
                    print(f'ERROR: {str(e)}')
                    return

                result_type = type(search["result"])
                if result_type is bool or result_type is int:
                    testExecutor = TestExecutor(self.config, self.config.server_settings)
                    testExecutor.process_test_result(search, result) 
                elif search["result"] == RETURN_RESULT:
                    return result
                elif search["result"] == DISPLAY_RESULT:
                    pretty_print(result)
                elif search["result"] == QUERY_AND_HITS_RESULT:
                    print(f'{str(result["meta"]["totalDocuments"]).rjust(8)} <== {result["query"]["query"]}')
                elif search["result"] == ID_LIST_RESULT:
                    pretty_print([doc["document"]["id"] for doc in result["result"]])
                elif search["result"] == ID_AND_CLASSIFER_RESULT:
                    search["exclude"] = ["content", "term", "location", "organization", "person", "email"]
                    for page in self.querier.get_search_result_pages(search):  
                        for doc in page:
                            fields = doc["document"]
                            output_line = str(fields["id"])
                            if search["classifier_field"] in fields:
                                output_line += "\t" + str(fields[search["classifier_field"]])
                            print(output_line)
                elif search["result"].startswith('save:'):
                    with open(search["result"][len(SAVE_RESULT):], 'wb') as f:
                        f.write(dumps(result, indent=4).encode('utf-8'))
                else:    
                    msg = f'"result" must be {", ".join(RESULT_TYPES)}, "{SAVE_RESULT}<file path>" or an int'
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
        print(f'The script "{script_name_with_ext}" takes exactly 1 parameter:\n')
        print(f'Usage:   python {script_name_with_ext} <config-file-path>')
        return
    file_path = cmd_line_args[1]
    if not isfile(file_path):
        print(f'"{file_path}" is not a file:\n')
        print(f'Usage:   python {script_name_with_ext} <config-file-path>')
        return

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
            print(f'Config file "{config_file_path}" contains bad json: {e}')
            return

        admin = Admin(Config(config))
        return admin.run_config()
        

run_from_cmd_line = False
if __name__ == '__main__':
    run_from_cmd_line = True
    run_cmd_line(sys.argv)


   
    