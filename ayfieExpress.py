# -*- coding: utf-8 -*-
#<#   <-- The instructions below refer to the first character on this line
"""
#>
# To use this script to install Python on Windows, do the following:
#
# > copy ayfieExpress.py installPython.ps1
#
# In the new file installPython.ps1, remove the first character of the 
# second line as instructed above before runninig the script:
#
# > powershell.exe .\installPython.ps1
#
# Upon complettion the original ayfie Express file can now be run
# as a normal Python script from a new terminal window:
#
# > python ayfieExpress.py 
#
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri "https://www.python.org/ftp/python/3.8.0/python-3.8.0-amd64.exe" -OutFile "python-3.8.0-amd64.exe"    
.\python-3.8.0-amd64.exe /quiet InstallAllUsers=0 PrependPath=1 Include_test=0
Exit
<#
"""

AYFIE_EXPRESS_VERSION = "0.0.99 (not uploaded)"

from json import loads, dumps
from json.decoder import JSONDecodeError
from socket import gaierror
from time import sleep, time
from datetime import datetime, date, timedelta
from os import remove
from os.path import join, dirname, exists, isdir, isfile, splitext, split as path_split, getsize, isabs, splitdrive, expanduser, abspath
from ntpath import basename
from os import listdir, makedirs, walk, system, stat, getcwd, chdir, environ
from zipfile import is_zipfile, ZipFile
from gzip import open as gzip_open
from tarfile import is_tarfile, open as tarfile_open 
from bz2 import decompress
from codecs import BOM_UTF8, BOM_UTF16_LE, BOM_UTF16_BE, BOM_UTF32_LE, BOM_UTF32_BE
from shutil import copy, copytree as copy_dir_tree, rmtree as delete_dir_tree, chown
from typing import Union, List, Optional, Tuple, Set, Dict, DefaultDict, Any
from re import search, match, sub, DOTALL, compile, findall
from random import random, shuffle, randint
from subprocess import run, PIPE, STDOUT, TimeoutExpired
from copy import deepcopy
from platform import system as operating_system
from collections import deque
from getpass import getuser
from inspect import signature
from argparse import ArgumentParser   
import contextlib
import logging
import sys
import csv
import warnings

# The f-formated string below will produce a syntax error and be displayed
# as part of the resulting error message if the version of Python is < 3.6
f"ERROR: ayfie Express requires Python 3.6 or later, please swap to later version"

max_int_value = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int_value)
        break
    except OverflowError:
        max_int_value = int(max_int_value/2) 

LOG_LEVEL_TO_USE      = logging.DEBUG

INSPECTOR             = "inspector"
SOLR                  = "solr"
ELASTICSEARCH         = "elasticsearch"

MAX_LOG_ENTRY_SIZE    = 2048

ROOT_OUTPUT_DIR       = f'{__file__}_output'
UNZIP_DIR             = join(ROOT_OUTPUT_DIR, 'unzipDir')
DATA_DIR              = join(ROOT_OUTPUT_DIR, 'dataDir')
LOG_DIR               = join(ROOT_OUTPUT_DIR, 'log')
DUMMY_DIR             = join(ROOT_OUTPUT_DIR, 'dummy')
DOWNLOAD_DIR          = join(ROOT_OUTPUT_DIR, 'inspectorDownload')
TMP_LOG_UNPACK_DIR    = join(ROOT_OUTPUT_DIR, 'tempLogUnpacking')
AYFIE_EXPRESS_CFG_DIR = join(ROOT_OUTPUT_DIR, 'configs')
CRASHED_BATCHES_DIR   = join(ROOT_OUTPUT_DIR, 'CrashedBatches')

OFF_LINE              = "off-line"

LOG_FILE_START_STRING = "Attaching to"
QUERY_API_LOGS        = "Query logs (Api Container)"
QUERY_SUGGEST_LOGS    = "Query logs (Suggest Container)"
YESTERDAY             = "yesterday"

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

BATCH_SIZE            = 1000
MAX_PAGE_SIZE         = 10000

DEL_ALL_COL           = "delete_all_collections"
DEL_COL               = "delete_collection"
CREATE_MISSING_COL    = "create_collection_if_not_exists"
RECREATE_COL          = "recreate_collection"
NO_ACTION             = "no_action"
PRE_FEEDING_ACTIONS   = [NO_ACTION, DEL_ALL_COL, DEL_COL, RECREATE_COL, CREATE_MISSING_COL]

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
HTML                  = 'html'
JSON_LINES            = 'json_lines'
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
    BOM_UTF32_BE: BOM_UTF32_BE,
}

BOM_MARKS_NAMES = {
    BOM_UTF8: 'utf-8-sig',  # <== This is required for CSV DictReader to work
    BOM_UTF16_LE: 'utf_16_le',
    BOM_UTF16_BE: 'utf_16_be',
    BOM_UTF32_LE: 'utf_32_le',
    BOM_UTF32_BE: 'utf_32_be'
}

MAX_CONNECTION_RETRIES = 10
MAX_PAUSE_BEFORE_RETRY = 120
SLEEP_LENGTH_FACTOR    = 5
CONNECTION_TIMEOUT     = 1800

NORMAL_RESULT          = "normal"
QUERY_AND_HITS_RESULT  = "query_and_numb_of_hits"
ID_LIST_RESULT         = 'id_list'
ID_AND_CLASSIFER_RESULT= 'id_and_classifier_score_list'
RESULT_TYPES           = [NORMAL_RESULT, QUERY_AND_HITS_RESULT, ID_LIST_RESULT, ID_AND_CLASSIFER_RESULT]
RETURN_RESULT          = 'return'
DISPLAY_RESULT         = 'display'
RESULT_DESTINATIONS    = [RETURN_RESULT, DISPLAY_RESULT] 

JOB_POLLING_INTERVAL   = 10

CLEAR_VALUES           = "clear"
REPLACE_VALUES         = "replace"
ADD_VALUES             = "add"
DOC_UPDATE_ACTIONS     = [CLEAR_VALUES, REPLACE_VALUES, ADD_VALUES]
K_FOLD_FIELD           = "k-fold-data-set"

COL_EVENT              = 'col_event'
COL_STATE              = 'col_state'
CLASSIFIER_STATE       = 'classifier_state'
WAIT_TYPES             = [COL_EVENT, COL_STATE, CLASSIFIER_STATE]

DOCKER_DOT_ENV_FILE             = ".env"
DOCKER_COMPOSE_FILE             = "docker-compose.yml"
DOCKER_COMPOSE_CUSTOM_FILE      = "docker-compose-custom.yml"
DOCKER_COMPOSE_METRICS_FILE     = "docker-compose-metrics.yml"
DOCKER_COMPOSE_FRONTEND_FILE    = "docker-compose-frontend.yml"
DOCKER_COMPOSE_SECURITY_FILE    = "docker-compose-security.yml"
DOCKER_COMPOSE_SELF_SIGNED_FILE = "docker-compose-security-self-signed.yml"
DOCKER_COMPOSE_PRIMARY_FILE     = "docker-compose-cluster-primary.yml"
DOCKER_COMPOSE_PROCESSOR_FILE   = "docker-compose-cluster-processor.yml"
DOCKER_COMPOSE_NETWORK_FILE     = "docker-compose-external-network.yml"
DOCKER_COMPOSE_FEEDER_FILE      = "docker-compose-feeder.yml"

APPLICATION_CUSTOM_FILE         = "application-custom.yml"
APPL_PII_TEMPLATE_FILE          = "application-pii.yml.template"
DOCKER_WINDOW_CONFIG_DIR        = "config"
SECURITY_ASSETS_DIR             = "security-assets"

ELASTICSEARCH_MAX_MEM_LIMIT     = 26
ELASTICSEARCH_MAX_MX_HEAP       = 16
MEM_LIMITS_64_AND_128_GB_RAM    = {
    "1": [
        ["AYFIE_MEM_LIMIT",         42, 84],
        ["ELASTICSEARCH_MEM_LIMIT", 10, 20],
        ["EXTRACTION_MEM_LIMIT",     4,  8],
        ["SUGGEST_MEM_LIMIT",        5, 10],
    ],
    "2": [
        ["API_MEM_LIMIT",           14, 18],
        ["ELASTICSEARCH_MEM_LIMIT",  9, 16],
        ["ELASTICSEARCH_MX_HEAP",    6, 10],
        ["SPARK_MASTER_MEM_LIMIT",   2,  2],
        ["SPARK_DRIVER_MEMORY",      6, 10],
        ["SPARK_WORKER_MEM_LIMIT",  28, 60],
        ["SPARK_WORKER_MEMORY",     24, 56],    
        ["SPARK_EXECUTOR_MEMORY",   24, 56],
        ["RECOGNITION_MEM_LIMIT",    4,  8],
        ["RECOGNITION_MX_HEAP",      2,  6],
        ["SUGGEST_MX_HEAP",          6,  6],
        ["SUGGEST_MEM_LIMIT",        7,  7],
    ] 
}   

OS_AUTO                = "auto"
OS_WINDOWS             = "windows"
OS_LINUX               = "linux"
SUPPORTED_OS           = [OS_WINDOWS, OS_LINUX]
SUPPORTED_OS_VALUES    = SUPPORTED_OS + [OS_AUTO]
DISTRO_CENTOS          = "centos"
DISTRO_UBUNTU          = "ubuntu"
DISTRO_REDHAT          = "redhat"
LINUX_DISTROS          = [DISTRO_CENTOS, DISTRO_REDHAT, DISTRO_UBUNTU]
DISTROS_USING_YUM      = [DISTRO_CENTOS, DISTRO_REDHAT]
APT_MANAGER            = "APT"
YUM_MANAGER            = "YUM"
DNF_MANAGER            = "DNF"
DISTROS_USING_APT      = [DISTRO_UBUNTU]
DISTROS_USING_DNF      = []
DOCKER_LINUX_DISTROS   = [DISTRO_CENTOS, DISTRO_UBUNTU]
DISTRO_NAME_MAP        = {
    "centos linux": DISTRO_CENTOS,
    "ubuntu": DISTRO_UBUNTU,
    "red hat enterprise linux": DISTRO_REDHAT
}
LINUX_DOCKER_REGISTRY  = "quay.io" 
WIN_DOCKER_REGISTRY    = "index.docker.io"   
PWD_START_STOP_TOKEN   = "$#$"

MONITOR_INTERVAL       = 60
EMAIL                  = "email"
SLACK                  = "slack"   
TERMINAL               = "terminal" 
ALERTS                 = "alerts"
HEARTBEATS             = "heartbeats"      
MESSAGE_TYPES          = [TERMINAL, EMAIL, SLACK]
NOTIFICATION_TYPES     = [ALERTS, HEARTBEATS]

TERMINAL_OUTPUT        = "terminal"
SILENCED_OUTPUT        = "silent"

DOCKER_COMPOSE_EXE     = "/usr/bin/docker-compose"

SELF_SIGNED_CERTIFICATE    = "self_signed"
LETS_ENCRYPT_CERTIFICATE   = "lets_encrypt"
COMMERCIAL_CERTIFICATE     = "commercial"
CERTIFICATE_TYPES          = [SELF_SIGNED_CERTIFICATE, LETS_ENCRYPT_CERTIFICATE, COMMERCIAL_CERTIFICATE]
FRONTEND_CONFIG_TEMPLATE   = "config-frontend-template.yml"
SECURE_API_CONFIG_TEMPLATE = "config-secure-api-template.yml"
CONFIG_TEMPLATE            = "config-template.yml"

BASH_START_SCRIPT          = "start-ayfie.sh"
BASH_STOP_SCRIPT           = "stop-ayfie.sh"
BASH_LOG_SCRIPT            = "ayfie-logs.sh"
INSPECTOR_BASH_SCRIPTS     = [BASH_START_SCRIPT, BASH_STOP_SCRIPT, BASH_LOG_SCRIPT]

CONDITIONAL_KEY            = "conditional"

ID_BASED_ON_FILENAME       = "filename"
ID_BASED_ON_SEQUENCE       = "sequence_number"
ID_GENERATION_ALGORITHMS   = [ID_BASED_ON_FILENAME, ID_BASED_ON_SEQUENCE]

HOST_OS  = operating_system().lower()
if HOST_OS == OS_LINUX:
    from grp import getgrall, getgrnam, getgrgid
    from pwd import getpwnam, getpwall
    from os import geteuid
elif HOST_OS == OS_WINDOWS:
    from ctypes import windll
else:
    raise OSError(f"'{HOST_OS}' is not a supported OS'") 
    
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
    
class BadZipFileError(Exception):
    pass
    
class CommandLineError(Exception):
    pass
    
class EngineDown(Exception):
    pass
    
class UnknownIssue(Exception):
    pass
    
class NotSupported(Exception):
    pass
    
class NotEnoughInformation(Exception):
    pass
    
# Dummy log to handle a cyclic dependency before before function prepare_logging() has completed
class DummyLog:
    def warning(self, msg):
        pass
    def debug(self, msg):
        pass
log = DummyLog()
    
def prepare_logging(): 
    Os.make_dir(ROOT_OUTPUT_DIR)
    Os.make_dir(LOG_DIR) 
    filepath = join(LOG_DIR, basename(basename(sys.argv[0])).split('.')[0] + ".log")
    Os.write_to_file_as_non_root_user(filepath, "".encode("utf-8"))
    global log
    log = logging.getLogger(__name__)
    logging.basicConfig(
        format = '%(asctime)s %(levelname)s: %(message)s, %(filename)s(%(lineno)d)',
        datefmt = '%m/%d/%Y %H:%M:%S',
        filename = filepath, 
        level = LOG_LEVEL_TO_USE 
    )
    
def replace_passwords(string):
    string_to_show = string
    string_fragments = string.split(PWD_START_STOP_TOKEN)
    if len(string_fragments) == 3:
        string_to_show = string_fragments[0] + "******" + string_fragments[2]
    string_to_use = "".join(string_fragments)
    return string_to_show, string_to_use 

def execute(cmd_line_str, dumpfile=None, timeout=None, continue_on_timeout=True, return_response=False, 
                          ignore_error=False, run_non_privileged=False, run_as_user=None, 
                          simulation=False, fake_response="", success_codes=[0], max_response_log_length=0):
    if simulation:
        string_to_show, string_to_use = replace_passwords(cmd_line_str)
        message = f"Simulating: '{string_to_show}'"
        if return_response:
            message += f" and the fake response: '{fake_response}'"
            print(message)
            return fake_response
        print(message)
        return
    stdout=PIPE
    stderr=STDOUT
    if dumpfile:
        stdout=dumpfile
        stderr=dumpfile
    process = None
    if HOST_OS == OS_LINUX and geteuid() == 0:
        user = None
        if run_as_user:
            user = run_as_user
        elif run_non_privileged:
            user = environ['SUDO_USER']
        if user:
            cmd_line_str = f"sudo -u {user} {cmd_line_str}"
    string_to_show, string_to_use = replace_passwords(cmd_line_str)
    log.debug(f"Command: '{string_to_show}'")
    try:
        process = run(string_to_use, timeout=timeout, shell=True, universal_newlines=True, stdout=stdout, stderr=stderr)
    except TimeoutExpired:
        log.warning(f'Command "{cmd_line_str}" timed out after {timeout} seconds.')
        if not continue_on_timeout:
            raise 
    if process:
        response = process.stdout.strip() if process.stdout else ""
        if len(response):
            single_line_response = response.replace('\n', '\\n')
            if max_response_log_length:
                if len(single_line_response) > max_response_log_length:
                    single_line_response = single_line_response[:max_response_log_length] + "(...)"
            log.debug(f"Response: '{single_line_response}'")
        if not process.returncode in success_codes and not ignore_error:
            if "Permission denied" in response:
                raise PermissionError(response)
            raise CommandLineError(f"Error (code: {process.returncode}): {response}")
        if return_response:
            return response
    else:
        raise UnknownIssue("No process object")


class Os:

    users = []

    ############ DISTRO ############
    @classmethod 
    def _get_env_vars_from_file(cls, filepath, name_var, version_var):
        try:
            with open(filepath, "r") as f:
                content = f.read()
        except:
            return None, None
        m = search(f"{name_var}=\"(.+?)\"", content, DOTALL)
        if m:
            name = m.group(1)
            m = search(f"{version_var}=\"(.+?)\"", content, DOTALL)
            if m:
                version = m.group(1)
                return name.lower(), version.lower()
        return None, None

    @classmethod    
    def get_linux_distro_and_version(cls):
        if HOST_OS != OS_LINUX:
            return None, None
        name, version = cls._get_env_vars_from_file("/etc/os-release", "NAME", "VERSION_ID")
        if not name:
            try:
                version = execute("lsb_release -sr", return_response=True)
                name = execute("lsb_release -si", return_response=True)
            except CommandLineError:
                pass
            if not name:
                name, version = cls._get_env_vars_from_file("/etc/lsb-release", "DISTRIB_ID", "DISTRIB_RELEASE")
                if not name:
                    name, version = cls._get_env_vars_from_file("/etc/debian_version", "DISTRIB_ID", "DISTRIB_RELEASE")
                    if not name:
                        raise AutoDetectionError("Unable to auto detect linux distro") 
        name = name.lower()
        if name in DISTRO_NAME_MAP:
            name = DISTRO_NAME_MAP[name]         
        log.debug(f"Linux distro '{name}', version: '{version}'")
        return name, version

    ############ USER ############
    @classmethod
    def is_root_or_elevated_mode(cls, simulate=False, fake_response=False):
        if simulate:
            return fake_response
        if HOST_OS == OS_LINUX:
            if geteuid() == 0:
                return True
            return False
        elif HOST_OS == OS_WINDOWS:
            if windll.shell32.IsUserAnAdmin() != 0:  
                return True
            return False
        else:
            raise NotSupported(f"Host OS '{HOST_OS}' is not a supported OS")
    
    @classmethod
    def get_actual_user(cls):
        if HOST_OS == OS_LINUX:
            if 'SUDO_USER' in environ:
                return environ['SUDO_USER']
            return getuser()
        elif HOST_OS == OS_WINDOWS:
            return getuser()
        else:
            raise NotSupported(f"Host OS '{HOST_OS}' is not a supported OS")
            
    @classmethod                
    def create_user(cls, user_name, password):
        if HOST_OS == OS_LINUX:
            if cls.user_exists(user_name):
                print(f"User '{user_name}' already exist")
            elif cls.is_root_or_elevated_mode():
                hashed_password = execute(f'echo "{password}" | openssl passwd -1 -stdin', return_response=True)
                execute(f'useradd -p {hashed_password} {user_name}', return_response=True)
                cls.users = []
            else:
                raise PermissionError(f"Need to be root user for creating user '{user_name}'")
                
    @classmethod          
    def delete_user(cls, user_name):
        if HOST_OS == OS_LINUX:
            if cls.user_exists(user_name):
                if cls.is_root_or_elevated_mode():
                    execute(f'userdel -r -f {user_name}')
                    cls.users = []
                else:
                    raise PermissionError(f"Need to be root user for deleting user '{user_name}'")
            else:
                raise ValueError(f"User '{user_name}' does not exist")

    @classmethod  
    def get_users(cls):
        if not cls.users:
            if HOST_OS == OS_LINUX:
                passwd_file_content = execute(f'cat /etc/passwd', return_response=True, max_response_log_length=100)
                cls.users = [line.split(':')[0] for line in passwd_file_content.split('\n')]
            elif HOST_OS == OS_WINDOWS:
                dir_blacklist = ['Default', 'Default User', 'Public', 'All Users']
                cls.users = [dir for dir in listdir(path_split(expanduser("~"))[0]) if not isfile(dir) and dir not in dir_blacklist]
            else:
                raise NotSupported(f"Host OS '{HOST_OS}' is not a supported OS")
        return cls.users

    @classmethod    
    def user_exists(cls, user):
        if user in cls.get_users():
            return True
        return False
            
    ############ GROUP ############
    @classmethod 
    def create_group(cls, group): 
        if HOST_OS == OS_LINUX:
            if not cls.group_exists(group):
                cmd = f"groupadd {group}" 
                log.debug(f'Creating group: "{cmd}"')
                execute(cmd)            

    @classmethod 
    def delete_group(cls, group, simulate=False):
        if HOST_OS == OS_LINUX:
            if cls.group_exists(group):
                cmd = f"groupdel {group}" 
                log.debug(f'Deleting group: "{cmd}"')
                execute(cmd, simulation=simulate)            

    @classmethod         
    def get_groups(cls):
        if HOST_OS == OS_LINUX:
            return [group[0] for group in getgrall()]
        return []
            
    @classmethod 
    def group_exists(cls, group, simulate=False, fake_response=False):
        if simulate:
            return fake_response
        if HOST_OS != OS_LINUX:
            raise NotImplementedError(f"There is no user group support for OS '{HOST_OS}'")
        try:
            getgrnam(group) 
            log.debug(f'User group "{group}" exists')
            return True
        except KeyError:
            log.debug(f'User group "{group}" does not exist')
            return False
    
    @classmethod 
    def add_user_to_groups(cls, user, groups, simulation=False):
        groups = listify(groups)
        if HOST_OS == OS_LINUX:
            for group in groups:
                if not cls.group_exists(group) or simulation:
                    cmd = f"groupadd {group}"
                    log.debug(f'Creating group: "{cmd}"')
                    execute(cmd, simulation=simulation)
                cmd = f"usermod -a -G {group} {user}"
                log.debug(f'Adding member to group: "{cmd}"')
                execute(cmd, simulation=simulation)

    @classmethod       
    def _get_primary_group(cls, user):
        if HOST_OS == OS_LINUX:
            if cls.user_exists(user):
                return getgrgid(getpwnam(user).pw_gid).gr_name
        return None                

    ############ FILES / DIRECTORIES ############        
    @classmethod
    def change_owner(cls, path, user=None, group=None, recursive=False):
        if HOST_OS == OS_LINUX:
            if exists(path):
                if not user:
                    user = cls.get_actual_user()
                if not group:
                    group = cls._get_primary_group(user)
                if recursive:
                    execute(f"chown {user}:{user} {path} -R")
                else:
                    chown(path, user, group)
                
    @classmethod                
    def change_mode(cls, path, mode, recursive=False):
        if HOST_OS == OS_LINUX:
            if exists(path):
                cmd = f"chmod {mode} {path}"
                if recursive:
                    execute(f"{cmd} -R")
                else:
                    execute(cmd)
        
    @classmethod
    def make_dir(cls, directory, user=None, simulate=False):
        if simulate:
            print(f"Simulating creating directory '{directory}'")
        elif not exists(directory):
            makedirs(directory)
            if HOST_OS == OS_LINUX and cls.is_root_or_elevated_mode:
                if not user:
                    user = cls.get_actual_user()
                cls.change_owner(directory, user)
     
    @classmethod
    def del_dir(cls, dir_path): 
        tries = 0
        while exists(dir_path):
            if tries > 5:
                raise IOError(f"Failed to delete directory '{dir_path}'")    
            delete_dir_tree(dir_path)
            tries += 1
            sleep(5 * tries)
        log.debug(f"Deleted directory '{dir_path}'")
            
    @classmethod
    def recreate_dir(cls, dir_path):
        cls.del_dir(dir_path)
        cls.make_dir(dir_path)  
        if not exists(dir_path):
            raise IOError("Failed to create directory '{dir_path}'") 

    @classmethod
    def write_to_file_as_non_root_user(cls, file_path, content, user=None):
        with open(file_path, 'wb') as f:
            f.write(content)
        cls.change_owner(file_path, user)
 
    @classmethod
    def get_script_extension(cls): 
        if HOST_OS == OS_LINUX:
            return "sh"
        elif HOST_OS == OS_WINDOWS:
            return "cmd"
        else:
            raise NotSupported(f"Host OS '{HOST_OS}' is not a supported OS")
 
 
def pretty_formated(input):
    obj = input
    if type(input) is str:
        if not len(input):
            raise ValueError('json string cannot be empty string')
        obj = loads(input)
    return dumps(obj, indent=4)    
    
def pretty_print(json):
    print(pretty_formated(json))
    
def listify(obj):
    if obj == None or obj == [] or obj == False:
        return []
    if not type(obj) is list:
        obj = [obj]
    return obj

def install_python_modules(modules):
    if not type(modules) is list:
        modules = [modules]
    first_time = True
    for module in modules:
        if ":" in module:
            package, module = module.split(":")
        else:
            package = module
        try:
            __import__(module)
        except ModuleNotFoundError:
            print(f"Python module '{module}' not installed. Installing now...")
            if first_time:
                first_time = False
                if HOST_OS == OS_LINUX:
                    execute(f"umask 022")
                command_line = f"{sys.executable} -m pip install --upgrade pip"
                try:
                    execute(command_line)
                except CommandLineError as e:
                    if "No module named pip" in str(e):
                        if HOST_OS == OS_LINUX and not Os.is_root_or_elevated_mode():
                            raise PermissionError(f"Installing '{module}' requires root user (sudo)")
                        linux_distro, version = Os.get_linux_distro_and_version()
                        if linux_distro in DISTROS_USING_APT:
                            execute("apt-get update")
                            execute("apt-get install python3-pip --assume-yes")
                        elif linux_distro in DISTROS_USING_YUM:
                            execute("yum install python3-pip --assume-yes")
                        try:
                            execute("pip3 --version")
                        except:
                            log.error("Failed to install pip3")
                            raise
                    else:
                        raise
            try:
                execute(f"{sys.executable} -m pip install {package}")  
                __import__(module)
            except:
                print(f"Failed to automatatically install '{package}.'")
                print("For some Linux distros this means one has to run the command line once for (almost) every module installed.")   
                sys.exit() 
                
def is_earlier_version(earlier_version, later_version):
    input_version_numbers = [earlier_version, later_version]
    version_numbers = []
    for version_number in input_version_numbers:
        split_version_number = version_number.split('.')
        if len(split_version_number) != 3:
            raise DataFormatError(f"The string '{version_number}' is not a valid version number") 
        version_numbers.append(split_version_number)
    for position in range(len(version_numbers[0])):
        try:
            if int(version_numbers[0][position]) < int(version_numbers[1][position]):
                return True
            if int(version_numbers[0][position]) > int(version_numbers[1][position]):
                return False
        except ValueError as e:      
            raise ValueError(f"Bad version number(s): {input_version_numbers}. Original error message: {str(e)}") 
    return False


prepare_logging()
install_python_modules(['requests', 'urllib3']) 
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@contextlib.contextmanager
def change_dir(dir):
    previous_dir = getcwd()
    chdir(dir)
    yield
    chdir(previous_dir)  

def get_unit_adapted_byte_figure(number_of_bytes):
    if number_of_bytes >= 1024 * 1024 * 1024 * 1024:
        number_of_bytes /= 1024 * 1024 * 1024 * 1024
        unit = "TB"
    elif number_of_bytes >= 1024 * 1024 * 1024:
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
    return round(number_of_bytes, 1), unit    
    

class WebContent():
        
    @classmethod
    def download(cls, url, directory=None):
        response = requests.get(url, allow_redirects=True)
        if response.status_code != 200:
            raise HTTPError(f'Downloading "{url}" failed with HTTP error code {response.status_code}')
        filename = cls._get_filename_from_response(response)
        if not filename:
            filename = url.split('/')[-1]
        if directory:
            path = join(directory, filename)
            Os.make_dir(directory)   
            Os.write_to_file_as_non_root_user(path, response.content)
            return path
        return response.content
    
    @classmethod    
    def _get_filename_from_response(self, response):
        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filenames = findall('filename=(.+)', content_disposition)
            if len(filenames) > 0:
                return filenames[0]
        return None


class FileTools():
 
    @classmethod 
    def get_file_type(cls, file_path):
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
                            return cls._get_text_format_type(start_byte_sequence, extension), detected_encoding
                        return FILE_SIGNATURES[pattern], detected_encoding
                return cls._get_text_format_type(start_byte_sequence, extension), detected_encoding
        return None, detected_encoding

    @classmethod 
    def _get_text_format_type(cls, start_byte_sequence, extension):
        start_char_sequence = start_byte_sequence.decode('iso-8859-1')
        org_start_char_sequence = start_char_sequence
        for char in [' ', '\n', '\r', '\t']:
            start_char_sequence = start_char_sequence.replace(char, '')
        char_patterns = {
            '{"documents":[{': AYFIE,
            '{"query":{'     : AYFIE_RESULT,
            '{"id":'         : JSON_LINES,
            '<!DOCTYPE html>': HTML,
            '<html'          : HTML,
            '<HTML'          : HTML
        }
        for pattern in char_patterns.keys():
            if start_char_sequence.startswith(pattern):
                return char_patterns[pattern]
            
        if start_char_sequence.startswith('{') and extension == '.json':
            return JSON
        elif extension == '.txt':
            return TXT
        elif extension in ['.htm', '.html']:
            return HTML
        elif extension == '.csv' or extension == '.tsv':
            try:
                dialect = csv.Sniffer().sniff(org_start_char_sequence)
                return CSV
            except:
                return None
        return None
        
    @classmethod 
    def _get_output_file_path(cls, input_file_path, zip_file_extension, unzip_dir):
        return join(unzip_dir, basename(input_file_path).replace(f".{zip_file_extension}", ""))

    @classmethod 
    def unzip(cls, file_path, unzip_dir, file_type=None):
        if not file_type:
            file_type, encoding = cls.get_file_type(file_path)
        if file_type not in ZIP_FILE_TYPES:
            return False
        if not unzip_dir:
            raise ValueError("No unzip directory is given")
        Os.recreate_dir(unzip_dir)
        if file_type == ZIP_FILE:
            with ZipFile(file_path, 'r') as f:
                f.extractall(unzip_dir)
                for file_path in f.namelist():
                    Os.change_owner(join(unzip_dir, file_path))
        elif file_type == TAR_FILE:
            with tarfile_open(file_path) as f:
                f.extractall(unzip_dir)
                for file_path in f.getnames():
                    Os.change_owner(join(unzip_dir, file_path))
        elif file_type == GZ_FILE:
            output_file_path = cls._get_output_file_path(file_path, "gz", unzip_dir)
            with gzip_open(file_path, 'rb') as f:
                Os.write_to_file_as_non_root_user(output_file_path, f.read())
        elif file_type == BZ2_FILE:
            output_file_path = cls._get_output_file_path(file_path, "bz2", unzip_dir)
            with open(file_path,'rb') as f:
                Os.write_to_file_as_non_root_user(output_file_path, f.read())
        elif file_type == _7Z_FILE:
            log.info(f"Skipping '{file_path}' - 7z decompression still not implemented")
        elif file_type == RAR_FILE:
            log.info(f"Skipping '{file_path}' - .rar file decompression still not implemented")
        else:
            raise ValueError(f'Unknown zip file type "{zip_file_type}"')
        return True
        
    @classmethod 
    def zip(cls, zip_file_path, source_files_root_dir, zip_file_type=ZIP_FILE, blocked_dirs=None, extension_filter=None, min_size=0, max_size=0):
        file_list = []
        if zip_file_type == ZIP_FILE:
            with ZipFile(zip_file_path, 'w') as z:
                for f in cls.get_next_file(source_files_root_dir, extension_filter, min_size, max_size):
                    file_list.append(f)
                    z.write(f)
        else:
            raise NotSupported(f"Zip file type '{zip_file_type}' is currently not supported")
        return file_list
           
    @classmethod 
    def gen_zipped_load_file_with_load(cls, load_file_path, zip_file_path, source_files_root_dir, zip_file_type=ZIP_FILE, 
                                            doc_id_generation=ID_BASED_ON_FILENAME, sequence_start_number=0, sequence_number_steps=1, 
                                            blocked_dirs=None, extension_filter=None, min_size=0, max_size=0, csv_dialect='excel'):
        doc_id = sequence_start_number
        rows = [("doc_id", "file_path")]
        if not doc_id_generation in ID_GENERATION_ALGORITHMS:
            raise ValueError(f"'{doc_id_generation}' is not a known id generation option")
        for file_path in cls.zip(zip_file_path, source_files_root_dir, zip_file_type, blocked_dirs, extension_filter, min_size, max_size):
            if doc_id_generation == ID_BASED_ON_FILENAME:
                path_without_extension, extension = splitext(file_path)
                doc_id = basename(path_without_extension)
            elif doc_id_generation == ID_BASED_ON_SEQUENCE:
                doc_id += sequence_number_steps
            drive, file_path = splitdrive(file_path)
            file_path = file_path.replace('/', '\\')
            if file_path[0] == '\\':
                file_path = file_path[1:] 
            rows.append((doc_id, file_path))
        with open(load_file_path, "w") as f:
            f.write("")
        with open(load_file_path, "a", newline="") as f:
            writer = csv.writer(f, csv_dialect)
            for row in rows:
                writer.writerow(row)
        with ZipFile(zip_file_path, 'a') as f:
            f.write(load_file_path)
        remove(load_file_path)
            
    @classmethod 
    def get_next_file(cls, root, blocked_dirs=None, extension_filter=None, min_size=0, max_size=0):
        items = listdir(root)
        files = [item for item in items if isfile(join(root, item))]
        dirs = [item for item in items if isdir(join(root, item))]
        ext_filter = []
        if extension_filter:
            for ext in [ext.lower() for ext in listify(extension_filter)]:
                if not ext.startswith('.'):
                    ext = '.' + ext
                    ext_filter.append(ext)
        for f in files:
            file_path = join(root, f)
            if min_size or max_size: 
                file_size = stat(file_path).st_size
                if min_size and file_size < min_size:
                    continue
                if max_size and file_size > max_size:
                    continue
            path_without_extension, extension = splitext(f)
            if ext_filter:
                if extension.lower() in ext_filter:
                    yield file_path
            else:
                yield file_path
        for dir in dirs:
            if blocked_dirs:
                if dir in blocked_dirs:
                    continue
            for f in cls.get_next_file(join(root, dir), blocked_dirs, ext_filter, min_size, max_size):
                yield f

    @classmethod 
    def _write_fragment_to_file(cls, dir_name, fragment_name, fragment_count, fragment_lines):
        path = join(dir_name, f"{fragment_name}_{fragment_count}")
        content = ''.join(fragment_lines).encode('utf-8')
        Os.write_to_file_as_non_root_user(path, content)
     
    @classmethod 
    def split_files(cls, dir_path, lines_per_fragment=0, max_fragments=0):
        for log_file in cls.get_next_file(dir_path):
            cls.split_file(log_file, lines_per_fragment, max_fragments)            
     
    @classmethod 
    def split_file(cls, file_path, lines_per_fragment=0, max_fragments=0):
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
                    cls._write_fragment_to_file(dir_name, fragment_name, fragment_count, fragment_lines)                    
                    line_count = 0
                    fragment_lines = []
                    fragment_count += 1
                    if max_fragments and fragment_count >= max_fragments:
                        return
                else:
                    line_count += 1
        if line_count > 0: 
            cls._write_fragment_to_file(dir_name, fragment_name, fragment_count, fragment_lines)
        
    @classmethod 
    def get_random_path(cls, path, name_prefix=""):
        if name_prefix:
            name_prefix += "-"
        return join(path, f"{name_prefix}{str(randint(10000000, 99999999))}")
    
    @classmethod 
    def get_absolute_path(cls, path, reference_path):
        if path:
            if path.startswith("http://") or path.startswith("https://"):
                return path
            if not isabs(path):
                return join(path_split(reference_path)[0], path)  
        return path
 
 
class RemoteServer():

    def __init__(self, server, username, port=22):
        install_python_modules(["paramiko", "scp"])
        import paramiko
        import scp
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        params = {
            "hostname": server,
            "username": username,
            "port": port
        }
        try: 
            self.ssh.connect(**params)
        except paramiko.ssh_exception.SSHException as e:
            raise paramiko.ssh_exception.SSHException(f"Pageant, putty or whatever ssh tool used does not currently seem to have access to the private key: '{str(e)}'")

    def _execute_command(self, command):
        stdin, stdout, stderr = self.ssh.exec_command(command)
        return stdout.read()

    def find_files(self, start_directory, file_pattern):
        response = self._execute_command(f'find {start_directory} -name "{file_pattern}"')
        return [ line.decode('utf-8') for line in response.splitlines()]

    def copy_file_from_remote(self, from_path, to_path, max_numb_of_retries=5):
        import scp
        retry = 0
        while retry < max_numb_of_retries:
            retry += 1
            with scp.SCPClient(self.ssh.get_transport()) as scp_client:
                try:
                    scp_client.get(from_path, to_path)
                except scp.SCPException as e:
                    pause = 60 * retry
                    print(f"Exception: '{str(e)}'. Trying again in {pause} seconds")
                    sleep(pause)
                
    def copy_file_to_remote(self, from_path, to_path):
        import scp
        with scp.SCPClient(self.ssh.get_transport()) as scp_client:
            scp_client.put(from_path, to_path)

   
class SearchEngine:

    @classmethod
    def get_function_signatures(self):
        signatures = []
        for method_name in dir(self):
            if method_name[0] != '_' and callable(getattr(self, method_name)):
                signatures.append(str(method_name + str(signature(getattr(self, method_name)))))
        return signatures

    def __init__(self, server, port, base_endpoint, headers, user=None, password=None, client_secret=None):
        self.server = server
        self.client_secret = client_secret
        self._set_port(port)
        self.base_endpoint = base_endpoint
        self.headers = headers
        self.statusCodes = self._getHTTPStatusCodes()
        self.user = user
        self.password = password
        self.last_batch = None
        self.batch_count = 0
        
    def get_last_batch(self):
        return self.batch_count, self.last_batch
        
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
            
    def _set_port(self, port):
        try:
            self.port = str(int(port))
        except ValueError:
            raise ValueError(f"Value '{port}' is not a valid port number")
        self.protocol = "http"
        if self.client_secret:
            self.port = '443'
        if self.port == '443':
            self.protocol = "https"
        
    def _get_endpoint(self, path, parameters):
        if parameters:
             parameters = f"?{ parameters}"
        if self.base_endpoint and self.base_endpoint[-1] != "/":
            self.base_endpoint += "/"
        port = f":{self.port}"
        if self.port in ["80", "443"]:
            port = ""
        return f'{self.protocol}://{self.server}{port}/{self.base_endpoint}{path}{parameters}'  

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
        msg += f'General error description: "{ayfieDescription}"'
        if err_msg != "null":
            msg += f'. Specific error description: "{err_msg}"'
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
        
    def _log_attempt_and_go_to_sleep(self, tries, error_msg, go_to_sleep, max_tries=3): 
        sleep_msg = ""
        if go_to_sleep:
            sleep_interval = tries * SLEEP_LENGTH_FACTOR
            if sleep_interval > MAX_PAUSE_BEFORE_RETRY:
                sleep_interval = MAX_PAUSE_BEFORE_RETRY 
            sleep(sleep_interval)
            sleep_msg = f". Trying again in {sleep_interval} seconds"
        msg = f"Connection issue: {error_msg}{sleep_msg}"
        log.debug(msg)
        if tries > max_tries:
            print(msg)
        
    def _response_format_converter(self, response_obj):
        return response_obj
        
    def _get_response_object(self, response):
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
        
    def _get_request_function_to_use(self, verb):   
        if self.user and self.password:
            session = requests.Session()
            session.auth = (self.user, self.password)
            return getattr(session, verb.lower())
        return getattr(requests, verb.lower())
        
    def _get_headers(self):
        headers = self.headers
        if self.client_secret:
            failed = True
            error_msg = ""
            endpoint = f'https://{self.server}/security/v1/client/token'
            data = {"clientId":"inspector","clientSecret": self.client_secret, "grantType":"client_credentials"}
            response = requests.post(endpoint, json=data, headers=headers, verify=False)
            response_obj = self._get_response_object(response)
            if "error" in response_obj:
                msg = response_obj['error']
                error_msg = f"Attempt to obtain access token fails: '{msg}'"
            elif "accessToken" in response_obj:
                access_token = response_obj["accessToken"]
                headers["Authorization"] = f"Bearer {access_token}"
                failed = False
            else:
                error_msg = f"No 'accessToken' parameter in response object: {response_obj}"
            if failed:
                error_msg += f", Headers: {self.headers}, Endpoint: {endpoint}, Data: {data}"
                log.error(error_msg)
        return headers
        
    def _execute(self, path, verb, data={}, parameters="", max_retry_attemps=MAX_CONNECTION_RETRIES):
        self._validateInputValue(verb, HTTP_VERBS)
        endpoint = self._get_endpoint(path, parameters) 
        request_function = self._get_request_function_to_use(verb.lower())
        log.debug(self._gen_req_log_msg(verb, endpoint, data, self.headers))
        if data and verb == "POST" and "batches" in endpoint:
            self.last_batch = dumps(data).encode('utf-8')
            self.batch_count += 1
            Os.write_to_file_as_non_root_user(join(ROOT_OUTPUT_DIR, "last_batch.json"), self.last_batch)
                
        tries = 0
        while True:
            if tries > max_retry_attemps:
                break
            tries += 1
            try:
                response = request_function(endpoint, json=data, headers=self._get_headers(), timeout=CONNECTION_TIMEOUT) 
                break
            except requests.exceptions.ConnectionError as e:
                if tries > max_retry_attemps:
                    raise
                self._log_attempt_and_go_to_sleep(tries, str(e), go_to_sleep=True)
            except (MemoryError, requests.exceptions.ReadTimeout) as e:
                self._log_attempt_and_go_to_sleep(tries, str(e), go_to_sleep=False, max_tries=1)
                raise 
            except Exception as e:
                print(str(e))
                if "!doctype html" in str(e) and "400" in str(e) and "bad request" in str(e).lower():
                    self._log_attempt_and_go_to_sleep(tries, "HTTP Status 400 – Bad Request", go_to_sleep=True)
                else:
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
        response_obj = self._get_response_object(response)
        if response.status_code >= 400:
            error_msg = self._gen_response_err_msg(response_obj, ayfieStatus)
            log.debug(error_msg)
            if response.status_code >= 500:
                failed_batch_path = join(ROOT_OUTPUT_DIR, f"failed-batch-{response.status_code}-error.json")
                Os.write_to_file_as_non_root_user(failed_batch_path, dumps(data).encode('iso-8859-1'))
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
        
    def engine_is_healthy(self):
        raise NotImplementedError
              
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

    def meta_search(self, col_id, json_query, retries=MAX_CONNECTION_RETRIES):
        raise NotImplementedError

    def json_based_meta_concept_search(self, col_id, json_query):
        raise NotImplementedError

    def doc_search(self, col_id, json_query):
        raise NotImplementedError

    def search_collection(self, col_id, query, size=20, offset=0, filters=[], exclude=[], aggregations=[], scroll=False, meta_data=False):
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

    def __init__(self, server, port, user=None, password=None, api_version=None, client_secret=None):     
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
        
    def meta_search(self, col_id, json_query, retries=MAX_CONNECTION_RETRIES):
        print('Info for later implementation: https://lucene.apache.org/solr/guide/7_1/json-request-api.html')
        return self._execute(f"{col_id}/select", HTTP_GET, {}, f"q={json_query['query']}", retries)
 
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

    def __init__(self, server, port, user=None, password=None, api_version=None, client_secret=None):
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
        
    def __init__(self, server, port, user=None, password=None, api_version='v1', client_secret=None):  
        headers = {'Content-Type':'application/hal+json'}
        SearchEngine.__init__(self, server, port, f"ayfie/{api_version}", headers, user, password, client_secret)
        
    def engine_is_healthy(self):
        try:
            response = self._execute('health', HTTP_GET)
        except HTTPError:
            return False
        if response:
            if (response['collections']['healthy'] and response['elasticsearch']['healthy'] 
                                                   and response['elasticsearch']['status'] == 'GREEN'
                                                   and response['initialization']['healthy']):
                return True
        return False

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

    def meta_search(self, col_id, json_query, retries=MAX_CONNECTION_RETRIES):
        path = f'collections/{col_id}/search'
        return self._execute(path, HTTP_POST, json_query, "", retries)

    def json_based_meta_concept_search(self, col_id, json_query):
        path = f'collections/{col_id}/search/concept'
        return self._execute(path, HTTP_POST, json_query)

    def doc_search(self, col_id, json_query):
        return self.meta_search(col_id, json_query)['result']

    def search_collection(self, col_id, query, size=20, offset=0, filters=[], exclude=[], aggregations=[], scroll=False, meta_data=False):
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
            
    ######### Suggest #########

    def get_query_suggestion(self, col_id, query, offset=None, limit=None, filter=None, retries=MAX_CONNECTION_RETRIES):
        parameters = f"query={query}"
        if offset != None:
            parameters += f"&offset={offset}"
        if limit != None:
            parameters += f"&limit={limit}"
        if filter:
            parameters += f"&filter={filter}"
        return self._execute(f'collections/{col_id}/suggestions?{parameters}', HTTP_GET, max_retry_attemps=retries)      
            
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

    def create_classifier(self, classifier_name, col_id, training_field, min_score, 
                                num_results, filters, use_entities=None, use_tokens=None):
        config = {
            "name": classifier_name,
            "id": classifier_name,
            "collectionId": col_id,
            "trainingClassesField": training_field,
            "minScore": min_score,
            "numResults": num_results,
            "filters": filters,
            "useEntities" : use_entities,
            "useTokens" : use_tokens
        }
        config = { k:v for k,v in config.items() if v != None }
        self._execute('classifiers', HTTP_POST, config)
        
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
         

class DataSource():

    def __init__(self, config, unzip_dir=UNZIP_DIR):
        self._init()
        self.config = config
        self.created_temp_data_dir = False
        self.unzip_dir = unzip_dir
        self.total_retrieved_file_count = 0
        self.total_retrieved_file_size = 0
        self.sequential_number = 1000000
        self.data_source_is_prepared = False
        
    def prepare_data_source(self):
        if self.data_source_is_prepared:
            return
        if self.config.data_source == None:
            raise ValueError(f"Parameter 'data_source' is mandatory (but it can be an empty string)")
        if self.config.data_source.startswith('http'):
            Os.recreate_dir(DOWNLOAD_DIR)
            self._print(f"Starting to download '{self.config.data_source}'...")
            self.config.data_source = WebContent.download(self.config.data_source, DOWNLOAD_DIR)
        if not exists(self.config.data_source):
            raise ValueError(f"There is no file or directory called '{self.config.data_source}'")
        if isdir(self.config.data_source):
            self.data_dir = self.config.data_source
        elif isfile(self.config.data_source):
            self.data_dir = FileTools.get_random_path(DATA_DIR)
            self.created_temp_data_dir = True
            Os.recreate_dir(self.data_dir)
            self._print("Starting to copy or to unzip data file...")
            if self._unzip(None, self.config.data_source, self.data_dir):
                self._print("Done unzipping data file")
            else:
                copy(self.config.data_source, self.data_dir)
                self._print("Done copying data file")
        else:
            msg = f'Source path "{self.config.data_source}" is neither a directory, '
            msg += 'a regular file nor a supported zip file type.'
            raise ValueError(msg)
        self.data_source_is_prepared = True
        
    def _print(self, message, end='\n'):
        if not self.config.silent_mode:
            print(message, end=end)
        log.info(message)
        
    def _init(self):
        self.doc_fields_key = "fields"

    def __del__(self):
        if self.created_temp_data_dir:
            Os.del_dir(self.data_dir)
            
    def _get_file_type(self, file_path):
        file_type, encoding = FileTools.get_file_type(file_path)
        if file_type == CSV:
            if self.config.treat_csv_as_text:
                file_type = TXT
        return file_type, encoding

    def _unzip(self, file_type, file_path, unzip_dir):
        return FileTools.unzip(file_path, unzip_dir, file_type)

    def _convert_pdf_to_text(self, file_path): 
        install_python_modules("PyPDF2")
        import PyPDF2
        with open(file_path, 'rb') as f:
            pdf = PyPDF2.PdfFileReader(f)
            pages = []
            for page_number in range(pdf.getNumPages()):
                pages.append(pdf.getPage(page_number).extractText())
            return ' '.join(pages)
            
    def _convert_html_to_text(self, file_path, encoding): 
        install_python_modules("html2text")
        import html2text
        with open(file_path, 'rb') as f:
            h = html2text.HTML2Text()
            h.ignore_links = True
            return h.handle(f.read().decode(encoding))

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
        
    def has_numeric_index_keys(self, mappings):
        try:
            _ = sum(list(mappings["fields"].values()) + [mappings["id"]])
            return True
        except TypeError:
            return False
        
    def _gen_doc_from_csv(self, csv_file, mappings, format):
        if not mappings:
            raise ConfigError("Configuration lacks a csv mapping table")
        for row in csv.DictReader(csv_file, **format):
            new_row = {}
            for k,v in mappings["fields"].items():
                values = v
                if not type(v) is list:
                    values = [v]
                token = "retrieve:"
                if len(values) == 1 and values[0].startswith(token):
                    id = row[mappings['id']]
                    if id:
                        new_row[k] = self._get_file_content(values[0][len(token):].replace("{id}", id))
                    continue             
                if self.config.include_headers_in_data:
                    new_row[k] = "\n".join([row[x] + " " + x for x in values])
                else:
                    new_row[k] = "\n".join([row[x] for x in values])
                    if type(v) is list:
                        new_row[k] = new_row[k].split(self.config.csv_list_value_delimiter)
            fields = {}
            for k,v in mappings["fields"].items():
                if (type(v) is not list and new_row[k]) or (type(v) is list and new_row[k][0]):
                    fields[k] = new_row[k]
            doc = {}
            if self.doc_fields_key:
                doc[self.doc_fields_key] = fields
            else:
                doc = fields
            doc["id"] = row[mappings['id']] if mappings['id'] else "seq-" + str(self.sequential_number)
            self.sequential_number += 1
            doc = self._add_other_fields(doc)
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
                if self.config.ignore_csv: 
                    log.info("Dropping file as not configured to process csv files")
                else:
                    for document in self._gen_doc_from_csv(f, self.config.csv_mappings, self.config.format):
                        yield document                    
            elif data_type == JSON_LINES:
                for document in self._get_file_content(f).split("\n"):
                    if len(document.strip()):
                        yield loads(document)
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
            elif data_type == HTML:
                id = self._gen_id_from_file_path(file_path)
                if self.config.convert_html_files: 
                    yield self._construct_doc(id, self._convert_html_to_text(file_path, encoding))
                else:
                    yield self._construct_doc(id, self._get_file_content(file_path))
            elif data_type in [WORD_DOC, XLSX, XML]:
                log.info(f"Skipping '{file_path}' - conversion of {data_type} still not implemented")
            elif data_type == TXT:
                yield self._construct_doc(self._gen_id_from_file_path(file_path), self._get_file_content(file_path))
            else:
                log.error(f"Unknown data type '{data_type}'")
                
    def _get_document_fragments(self, document):
        if self.config.fragmented_doc_dir:
            Os.make_dir(self.config.fragmented_doc_dir) 
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
                    Os.write_to_file_as_non_root_user(join(self.config.fragmented_doc_dir, split_document["id"] + ".json"), 
                                        pretty_formated(split_document).encode("utf-8"))  
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
                    self._print(f'Document with id "{document["id"]}" is a multipart email')
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
            Os.recreate_dir(unzip_dir)
            log.debug(f"Created output directory '{unzip_dir}' for zip file '{file_path}'")
            self._unzip(file_type, file_path, unzip_dir)
            for unzipped_file in FileTools.get_next_file(unzip_dir, None, self.config.file_extension_filter, 
                                                                   self.config.min_doc_size, self.config.max_doc_size):
                file_type, self.detected_encoding = self._get_file_type(unzipped_file)
                log.debug(f"'{unzipped_file}' auto detected as '{file_type}'")
                if self.config.file_type_filter and not file_type in self.config.file_type_filter:
                    log.debug(f"'{file_path}' auto detected as '{file_type} and not excluded due to file type filter: {self.config.file_type_filter}")
                    continue
                if file_type in ZIP_FILE_TYPES:
                    sub_unzip_dir = FileTools.get_random_path(unzip_dir)
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
            Os.del_dir(unzip_dir)
            log.debug(f"Deleting directory '{unzip_dir}'")

    def _get_unsplit_documents(self):
        self.file_size = {}
        self.file_paths = []
        for file_path in FileTools.get_next_file(self.data_dir, None, self.config.file_extension_filter, 
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
        
    def _add_preprocess_field(self, document):
        if self.config.preprocess:
            if self.config.preprocess in PREPROCESS_TYPES:
                document["fields"]['preprocess'] = self.config.preprocess
            else:
                raise ConfigError(f"Unknown preprocess type {preprocess}")
        return document
                
    def _construct_doc(self, id, content):
        if self._drop_due_to_size(id, content):
            return None  
        doc = {
            "id": id,
            "fields": {
                "content": content
            }
        }
        doc = self._add_other_fields(doc)
        return doc
        
    def _add_other_fields(self, doc):
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

    def _pre_init(self):
        pass
        
    def _post_init(self):
        pass
    
    def __init__(self, config, data_source=None):
        self.config = config
        self.data_source = data_source
        self._pre_init()
        args = [
            self.config.server, 
            self.config.port, 
            self.config.user, 
            self.config.password, 
            self.config.api_version, 
            self.config.client_secret
        ]
        if self.config.engine == INSPECTOR:
            self.engine = Inspector(*args)
        elif self.config.engine == SOLR:
            self.engine = Solr(*args)
        elif self.config.engine == ELASTICSEARCH:
            self.engine = Elasticsearch(*args)
        else:
            raise ValueError(f"Engine {self.config.engine} is not supported")
        self._post_init()
        
    def _report_time(self, operation, elapsed_second):        
        message = f"The {operation} operation took {str(elapsed_second)} seconds"
        log.info(message)
        self._print(message)
                 
    def _print(self, message, end='\n'):
        if not self.config.silent_mode:
            print(message, end=end)
            
    def get_doc_ids(self, col_id, query="", limit=None, random_order=False, filters=[]):
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


class DockerFile:

    def __init__(self, install_dir, yaml_version=None, os=None, filename=None):
        self.install_dir = install_dir
        self.yaml_version = yaml_version
        self.os = HOST_OS
        if os:
            self.os = os
        if not self.os in SUPPORTED_OS:
            raise ValueError(f"'{self.os}' is not a valid OS") 
        self.filename = filename
        if not self.filename:
            raise ValueError("Name of docker related file not set")
        self.json_data = []
        self.yaml_data = []
        self.data_dict = {}

    def _write_to_file(self, content, directory=None):
        if not directory:
            directory = self.install_dir if exists(self.install_dir) else "."
        Os.make_dir(directory)
        with change_dir(directory):
            Os.write_to_file_as_non_root_user(self.filename, content.encode("utf-8"))
        return join(directory, self.filename)

           
class DockerEnvFile(DockerFile):  

    def __init__(self, install_dir, version, config, os=None, filename=None): 
        self.version = version
        if not filename:
            filename = ".env"
        super().__init__(install_dir, None, os, filename)
        install_python_modules(['numpy', 'PyYAML:yaml']) 
        self.config = config
        
    def _print(self, message):
        if not self.config.silent_mode:
            print(message)
        log.info(message)
        
    def _add_docker_compose_yml_file(self, enable_feature, custom_file):       
        if enable_feature:
            if not custom_file in self.docker_compose_custom_files:
                self.docker_compose_custom_files.append(custom_file)
                
    def _remove_docker_compose_yml_file(self, custom_file, first_version_to_not_remove=None):
        remove_file = False
        if first_version_to_not_remove:
            if self.version and is_earlier_version(self.version, first_version_to_not_remove):
                remove_file = True
        else:
            remove_file = True
        if remove_file and custom_file in self.docker_compose_custom_files:
            self.docker_compose_custom_files.remove(custom_file)
            
    def _get_security_parameters(self, security, certificate_type, valid_keys):
        parameters = {}
        if security:
            if not "type" in security:
                raise ValueError("The security settings has no 'type' parameter")
            cert_type = security["type"]
            if not cert_type in CERTIFICATE_TYPES:
                raise ValueError(f"'{cert_type}' is an unknown certificate type")
            if cert_type != certificate_type:
                return {}
            for key in valid_keys:
                if not key in security:
                    raise ValueError(f"A '{cert_type}' certificate requires the field '{key}' to be set")
                value = security[key]
                if type(value) is list:
                    value = ",".join(value)
                parameters[key] = value
        return parameters
        
    def _prepare_install_or_env_file_creation(self):
        self.self_sign_certificate = {}
        self.lets_encrypt_certificate = {}
        if self.config.security:
            self.self_sign_certificate = self._get_security_parameters(self.config.security, SELF_SIGNED_CERTIFICATE,
                                                                        ["domain", "country", "state", "city", "company"])
            self.lets_encrypt_certificate = self._get_security_parameters(self.config.security, LETS_ENCRYPT_CERTIFICATE,
                                                                        ["domain", "acme_domains", "acme_email"])
            if str(self.config.port) != "443": 
                raise ValueError("The port has to be set to 443 when security is enabled (both 80 and 443 will be used)")
        is_primary_node = False 
        self.is_processor_node = False 
        if self.config.primary_node and self.config.processor_node:
            raise ValueError("A node can be a primary node or a processor node, not both")
        if self.config.processor_node and not self.config.primary_node_IP_or_FQDN:
            raise ValueError("A processor node must be configured with the IP or the FQDN of the primary node")
        if is_earlier_version(self.version, "2.9.0"): 
            self._print("Starting a single node installation..")
        elif self.config.primary_node:        
            is_primary_node = True
            self._print("Starting a primary node installation..")
        elif self.config.processor_node:
            self.is_processor_node = True
            self._print(f"Starting a processor node installation (primary node at '{self.config.primary_node_IP_or_FQDN}')...")
        else:
            self._print("Starting a single node installation...") 
        self.docker_compose_custom_files = self.config.docker_compose_yml_files 
        self._add_docker_compose_yml_file(self.config.external_network, DOCKER_COMPOSE_NETWORK_FILE)
        if self.is_processor_node:
            self._add_docker_compose_yml_file(self.is_processor_node, DOCKER_COMPOSE_PROCESSOR_FILE)
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_FILE)
        else:
            self._add_docker_compose_yml_file(self.config.enable_frontend, DOCKER_COMPOSE_FRONTEND_FILE)
            self._add_docker_compose_yml_file(self.config.enable_grafana, DOCKER_COMPOSE_METRICS_FILE)
            self._add_docker_compose_yml_file(self.config.enable_feeder, DOCKER_COMPOSE_FEEDER_FILE)
            self._add_docker_compose_yml_file(self.config.security, DOCKER_COMPOSE_SECURITY_FILE)
            self._add_docker_compose_yml_file(self.self_sign_certificate, DOCKER_COMPOSE_SELF_SIGNED_FILE)
            self._add_docker_compose_yml_file(is_primary_node, DOCKER_COMPOSE_PRIMARY_FILE)
        if self.os == OS_WINDOWS:
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_FRONTEND_FILE, "2.6.1")
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_SECURITY_FILE)
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_SELF_SIGNED_FILE)   
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_METRICS_FILE)
        elif self.os == OS_LINUX:
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_SECURITY_FILE, "2.8.0")
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_SELF_SIGNED_FILE, "2.8.0")
            self._remove_docker_compose_yml_file(DOCKER_COMPOSE_METRICS_FILE, "2.0.3")
        self._remove_docker_compose_yml_file(DOCKER_COMPOSE_FEEDER_FILE, "2.10.0")
        self.report_container_status()
        
    def report_container_status(self):
        enabled_components = []
        disabled_components = []
        components = [
            (DOCKER_COMPOSE_FRONTEND_FILE, "Frontend"), 
            (DOCKER_COMPOSE_SECURITY_FILE, "Security"), 
            (DOCKER_COMPOSE_METRICS_FILE, "Grafana"), 
            (DOCKER_COMPOSE_FEEDER_FILE, "Feeder (Enrichor)"),            
        ]
        for component in components:
            if component[0] in self.docker_compose_custom_files:
                enabled_components.append(component[1])
            else:
                disabled_components.append(component[1])
        self._report_component_status(enabled_components, "enabled")
        self._report_component_status(disabled_components, "disabled")

    def _report_component_status(self, component_list, component_status):
        if len(component_list):
            msg = ""
            if len(component_list) > 2: 
                msg = f"{', '.join(component_list[:-2])}, "
            msg += f"{' and '.join(component_list[-2:])} {component_status}"
            self._print(msg)
        
    def add_variable(self, name, value):
        self.data_dict[name] = value
        
    def _get_mem_limit(self, limit_64_ram, limit_128_ram, ram_on_host):
        import numpy
        coefs, _, _, _, _ = numpy.polyfit([64, 128], [limit_64_ram, limit_128_ram], deg=1, full=True)
        mem_limit = int(round(ram_on_host * coefs[0] + coefs[1]))
        if mem_limit < self.minimum_ram_allocation:
            mem_limit = self.minimum_ram_allocation
        return mem_limit
        
    def _gen_mem_limits(self):
        main_version = self.version.split(".")[0]
        if not main_version in ["1", "2"]:
            raise ValueError(f"There is no main version number '{str(main_version)}' of the ayfie Inspector")
        self.minimum_ram_allocation = 1
        freed_up_ram = 0
        for variable, limit_64_ram, limit_128_ram in MEM_LIMITS_64_AND_128_GB_RAM[main_version]:
            if variable == "SUGGEST_MEM_LIMIT" and self.config.no_suggest_index:
                freed_up_ram += self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host)
                freed_up_ram -= self.minimum_ram_allocation
            if variable == "ELASTICSEARCH_MEM_LIMIT":
                mem_limit = self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host)
                if mem_limit > ELASTICSEARCH_MAX_MEM_LIMIT:
                    freed_up_ram += mem_limit - ELASTICSEARCH_MAX_MEM_LIMIT
        mem_limits = []
        for variable, limit_64_ram, limit_128_ram in MEM_LIMITS_64_AND_128_GB_RAM[main_version]:
            if self.config.no_suggest_index and "SUGGEST" in variable:
                mem_limit = self.minimum_ram_allocation
            else:
                mem_limit = self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host + freed_up_ram)
                if variable == "ELASTICSEARCH_MEM_LIMIT" and mem_limit > ELASTICSEARCH_MAX_MEM_LIMIT:
                    mem_limit = ELASTICSEARCH_MAX_MEM_LIMIT
                if variable == "ELASTICSEARCH_MX_HEAP" and mem_limit > ELASTICSEARCH_MAX_MX_HEAP:
                    mem_limit = ELASTICSEARCH_MAX_MX_HEAP
            self.add_variable(variable, f"{mem_limit}G") 
        
    def _get_lets_encrypt_certificate_dot_env_settings(self):
        if self.lets_encrypt_certificate: 
            for key in self.lets_encrypt_certificate:
                value = self.lets_encrypt_certificate[key]
                if type(value) is list:
                    value = ",".join(value)
                self.add_variable(key.upper(), value)
        
    def _get_self_sign_certificate_dot_env_settings(self):
        if self.self_sign_certificate:
            if "domain" in self.self_sign_certificate:
                self.add_variable("DOMAIN", self.self_sign_certificate["domain"])
        
    def _get_processor_node_dot_env_settings(self):
        if self.is_processor_node:
            self.add_variable("SERVICES_SUGGEST_URL", f"http://{self.config.primary_node_IP_or_FQDN}:9090")
            self.add_variable("SERVICES_RECOGNITION_URL", f"http://{self.config.primary_node_IP_or_FQDN}:2323")
            self.add_variable("MESSAGING_HOST", self.config.primary_node_IP_or_FQDN)
            self.add_variable("ELASTICSEARCH_HOST", self.config.primary_node_IP_or_FQDN)
        
    def order_yml_file_list(self, unordered_yml_file_list):
        ordered_list = []
        tail_items = []
        unordered_yml_file_list = list(set(unordered_yml_file_list))
        for item in unordered_yml_file_list:
            if "security.yml" in item:
                tail_items = [item] + tail_items
            elif "signed.yml" in item:
                tail_items = tail_items + [item]
            else:
                ordered_list.append(item)
        return ordered_list + tail_items
        
    def _merge_yaml_dict_structures(self, existing, addition, path=None):
        if path is None: 
            path = []
        for key in addition:
            if key in existing:
                if isinstance(existing[key], dict) and isinstance(addition[key], dict):
                    self._merge_yaml_dict_structures(existing[key], addition[key], path + [str(key)])
                elif existing[key] == addition[key]:
                    pass
                else:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
            else:
                existing[key] = addition[key]
        return existing  
        
    def _gen_content(self):
        custom_files = [DOCKER_COMPOSE_FILE]
        docker_compose_custom_files = self.docker_compose_custom_files[:]
        if self.config.enable_gdpr or self.config.enable_frontend:
            docker_compose_custom_files.append(DOCKER_COMPOSE_CUSTOM_FILE)
        self.add_variable("COMPOSE_FILE", self.order_yml_file_list(docker_compose_custom_files))
        if self.config.security:
            self.add_variable("AYFIE_PORT", "80")
            self.add_variable("AYFIE_HTTPS_PORT", "443")
        else:
            self.add_variable("AYFIE_PORT", self.config.port)
        self._gen_mem_limits()
        self._get_lets_encrypt_certificate_dot_env_settings()
        self._get_self_sign_certificate_dot_env_settings()
        self._get_processor_node_dot_env_settings()

    def generate_file(self):
        self._prepare_install_or_env_file_creation()
        self._gen_content()
        if self.os == OS_LINUX:
            delimiter = ":"
        elif self.os == OS_WINDOWS:
            delimiter = ";"   
        content = ""
        for name in self.data_dict:
            value = self.data_dict[name]
            if type(value) is list:
                value = delimiter.join(value)
            content += f"{name}={value}\n"
        return self._write_to_file(content, self.install_dir)        

        
class DockerYamlFile(DockerFile):

    def __init__(self, install_dir, yaml_version=None, os=None, filename=None):
        install_python_modules(['numpy', 'PyYAML:yaml'])
        super().__init__(install_dir, yaml_version, os, filename)
        
    def _convert_json_to_yaml(self, json_content):
        import yaml
        return yaml.dump(json_content, default_flow_style=False)

    def _write_json_as_yaml_to_file(self, json_content): 
        self._write_yaml_to_file(self, self._convert_json_to_yaml(json_content, default_flow_style=False))
                  
    def _write_yaml_to_file(self, yml_content, directory=None):
        if yml_content:
            self._write_to_file(f"version: '{self.yaml_version}'\n{yml_content}", directory)
        
    def generate_file(self):
        self._write_yaml_to_file(self._get_yml_content())
        
    def _get_yml_content(self):
        raise NotImplementedError
        

class DockerComposeCustomYamlFile(DockerYamlFile):

    def __init__(self, install_dir, yaml_version=None, os=None, filename=None):
        super().__init__(install_dir, yaml_version, os, DOCKER_COMPOSE_CUSTOM_FILE)
        self.data = []
        
    def add_file_copy_operation(self, service, external_file_path, internal_file_path):
        self.data.append({
            "service": service,
            "external_file_path": external_file_path, 
            "internal_file_path": internal_file_path
        })
        
    def _get_yml_content(self):
        indent = "  "
        content = "services:\n"
        for copy_operation in self.data:
            content += f'{indent}{copy_operation["service"]}:\n'
            content += f'{indent}{indent}volumes:\n'
            content += f'{indent}{indent}{indent}- {copy_operation["external_file_path"]}:{copy_operation["internal_file_path"]}\n'
        return content

        
class ApplicationYamlFile(DockerYamlFile):

    def __init__(self, install_dir, yaml_version=None, os=None, filename=None):
        super().__init__(install_dir, yaml_version, os, APPLICATION_CUSTOM_FILE)
        self.dockerComposeCustomYamlFile = None
        self.container_names = []

    def _configure_extractors_to_skip(self, extractors_to_skip):
        self.json_data.append({
            "digestion": {
                "processing": {
                    "entityExtraction": {
                        "skipExtractors": extractors_to_skip
                    }
                }
            }
        })
        
    def _configure_email_treading(self, skipCloneEvaluation, skipMailEvaluation, mailTreadEvaluation):
        self.json_data.append({
            "digestion": {
                "processing": {
                    "cloneEvaluation": {
                        "skip": skipCloneEvaluation
                    },
                    "mailEvaluation": {
                        "skip": skipMailEvaluation
                    },
                    "mailThreadEvaluation": {
                        "skip": mailTreadEvaluation
                    }
                }
            }
        })
        
    def _configure_admin_page(self, hideNavbar, admin_visible, jobs, collections):
        self.json_data.append({
            "frontendConfig": {
                "hideNavbar": hideNavbar, 
                "adminConfig" : {
                    "visible": admin_visible,
                    "jobs": jobs,
                    "collections": collections
                }
            }
        })
        
    def _read_yaml_segment_from_file(self, filename):
        with change_dir(self.install_dir):
            with open(filename, "rb") as f:
                self.yaml_data.append(f.read().decode("utf-8"))
                
    def _get_yml_content(self):
        content = ""
        if self.json_data or self.yaml_data:
            indent = "  "
            content += f"services:\n"
            for json_data in self.json_data:
                content += self._convert_json_to_yaml(json_data) + "\n"
            for yaml_data in self.yaml_data:
                content += yaml_data
        return content
 
    def _copy_application_file_into_container(self, container_name):
        if container_name in self.container_names:
            return
        self.container_names.append(container_name)
        if not self.dockerComposeCustomYamlFile:
            self.dockerComposeCustomYamlFile = DockerComposeCustomYamlFile(self.install_dir, self.yaml_version, os=self.os)
        if self.os == OS_LINUX:
            from_path = f"./{APPLICATION_CUSTOM_FILE}"
            if container_name == "api":
                to_path = "/home/dev/restapp/application-custom.yml"
            elif container_name == "frontend":
                to_path = "/dist/browser/custom-config/application-custom.yml"
        elif self.os == OS_WINDOWS: 
            from_path = f"./{DOCKER_WINDOW_CONFIG_DIR}"
            to_path = "c:\\ayfie\\config" 
        else:
            raise ValueError(f"'{self.os}' is not a valid OS")
        self.dockerComposeCustomYamlFile.add_file_copy_operation(container_name, from_path, to_path)
        
    def enable_gdpr(self):
        self._read_yaml_segment_from_file(APPL_PII_TEMPLATE_FILE)      
        self._configure_extractors_to_skip([]) 
        self._copy_application_file_into_container("api")
        
    def enable_email_treading(self):
        self._configure_email_treading(False, False, False)
        self._copy_application_file_into_container("api")
        
    def enable_admin_page(self):
        self._configure_admin_page(False, True, True, True)
        self._copy_application_file_into_container("frontend")

    def generate_file(self):
        if self.os == OS_WINDOWS:
            self._write_yaml_to_file(self._get_yml_content(), join(self.install_dir, DOCKER_WINDOW_CONFIG_DIR))
        elif self.os == OS_LINUX:
            self._write_yaml_to_file(self._get_yml_content())
        else:
            raise ValueError(f"'{self.os}' is not a supported OS")
        if self.dockerComposeCustomYamlFile:
            self.dockerComposeCustomYamlFile.generate_file()                 


class Application:

    def __init__(self, simulate_execution=False, simulation_config={}, silent=False):
        self.count = 0
        self.simulate_execution = simulate_execution
        self.simulation_config = deepcopy(simulation_config)
        self.original_simulation_config = deepcopy(simulation_config)
        self.silent = silent
        self.requires_restart = False
        if self.simulate_execution:
            if not self.simulation_config:
                self.simulation_config["installed"] = False
                self.simulation_config["running"] = False
                self.simulation_config["os"] = HOST_OS
                self.simulation_config["linux_distro"] = None
                if HOST_OS == OS_LINUX:
                    self.simulation_config["linux_distro"], _ = Os.get_linux_distro_and_version()
            self._print(f"Simulation turned ON for {self.APPLICATION_NAME} with these initial simulated states/settings:")
            self._pretty_print(self.simulation_config)
            self.os = self.simulation_config["os"]
            self.linux_distro = self.simulation_config["linux_distro"] 
        else:
            self.os = HOST_OS
            if self.os == OS_LINUX:
                self.linux_distro, _ = Os.get_linux_distro_and_version()
            else:
                self.linux_distro = None
        if self.linux_distro:
            if self.linux_distro in DISTROS_USING_APT:
                self.package_manager = APT_MANAGER
            elif self.linux_distro in DISTROS_USING_YUM:
                self.package_manager = YUM_MANAGER
            else:
                raise NotImplementedError(f"Distro '{self.linux_distro}' is not supported")
                
    def install(self):
        if self._install_is_verifiable() and self._is_installed():
            self._print(f"{self.APPLICATION_NAME} already installed...")
            return
        self._prerequisites()
        self._install()
        self.simulation_config["installed"] = True
        if not self.requires_restart:
            if self._install_is_verifiable() and not self._is_installed():
                raise BadStateError(self._get_logged_msg(f"Failed to install {self.APPLICATION_NAME}"))
        self._post_install()
        
    def _install(self):
        raise NotImplementedError()
        
    def _install_is_verifiable(self):
        return True
        
    def _running_is_verifiable(self):
        return False
       
    def _is_installed(self):
        raise NotImplementedError
        
    def _prerequisites(self):
        pass
        
    def _post_install(self):
        pass
    
    def uninstall(self):
        if self._install_is_verifiable() and not self._is_installed():
            self._print(f"{self.APPLICATION_NAME} not installed...")
            return
        if self._running_is_verifiable() and self._is_running():
            raise BadStateError(self._get_logged_msg(f"{self.APPLICATION_NAME} must be stopped before it can be uninstalled."))
        self._print(f"Uninstalling {self.APPLICATION_NAME}...")
        self._uninstall()
        self.simulation_config["installed"] = False
        if self._install_is_verifiable() and self._is_installed():
            raise BadStateError(self._get_logged_msg(f"Failed to uninstall {self.APPLICATION_NAME}."))
            
    def _uninstall(self):
        raise NotImplementedError()
        
    def start(self):
        pass
        
    def stop(self):
        pass
    
    def _print(self, message):
        if not self.silent:
            print(message)
        log.info(message)
        
    def _pretty_formated(self, input):
        obj = input
        if type(input) is str:
            if not len(input):
                raise ValueError('json string cannot be empty string')
            obj = loads(input)
        return dumps(obj, indent=4)    
        
    def _pretty_print(self, input):
        self._print(self._pretty_formated(input))
 
    def _write_to_file(self, filepath, content, mode="w", encoding=None):
        if self.simulate_execution:
            self._print(f"Simulating file write operation to file: {filepath}")
            self._print(f"---------------\n{content}\n---------------")
        else:
            with open(filepath, mode) as f:
                if encoding:
                    content = content.encode(encoding)
                f.write(content)
            
    def _pretty_write_to_file(self, filepath, content, mode="w", encoding=None):
        self._write_to_file(filepath, self._pretty_formated(content), mode, encoding)
        
    def _read_from_file(self, filepath, mode="r", encoding=None, fake_content=None):
        if self.simulate_execution:
            self._print(f"Simulating read operation from file '{filepath}':")
            self._print(f"---------------\n{fake_content}\n---------------")
            content = fake_content
        elif exists(filepath):
            with open(filepath, mode) as f:
                content = f.read()
                if encoding:
                    content = content.decode(encoding)
        else:
            content = ""
        return content
        
    def _read_json_from_file(self, filepath, mode="r", encoding=None, fake_content=None):
        content = self._read_from_file(filepath, mode, encoding, fake_content)
        return loads(content) if len(content) else {}
        
    def _make_dir(self, directory):
        Os.make_dir(directory, simulate=self.simulate_execution)
         
    @classmethod
    def gen_simulation_config(cls, installed=False, running=False, os=OS_LINUX, linux_distro=DISTRO_UBUNTU):
        return {
            "installed": installed,
            "running": running,
            "os": os,
            "linux_distro": linux_distro
        }
        
    def change_simulation_config(self, config):
        for key in config:
            self.simulation_config[key] = config[key]
            
    def get_current_simulation_config(self):
        return self.simulation_config
        
    def _execute(self, *args, **kwargs):
        return execute(*args, simulation=self.simulate_execution, **kwargs)
        
    def _get_logged_msg(self, msg, log_level="ERROR"):
        log_level = log_level.lower()
        if log_level == "error":
            log.error(msg)
        return msg
        
    def is_requiring_restart(self):
        return self.requires_restart
        h
    def _restart_server(self):
        if self.os == OS_WINDOWS:
            args = ' '.join(sys.argv[1:])
            input("Press Enter to reboot the server. The script will automtically restart.")
            runonce_key = r"HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce"
            reg_entry = f'reg add {runonce_key} /v ayfie_installer /t REG_SZ /d "{abspath(__file__)} {args}" /f'
            self._execute(reg_entry)
            self._execute("powershell.exe Restart-Computer")
 
 
class Daemon(Application): 

    def _running_is_verifiable(self):
        return True

    def start(self):
        if self._install_is_verifiable() and not self._is_installed(): 
            raise BadStateError(self._get_logged_msg(f"{self.APPLICATION_NAME} not installed"))
        if self._running_is_verifiable() and self._is_running():
            self._print(f"{self.APPLICATION_NAME} running...")
            return
        if not Os.is_root_or_elevated_mode():
            raise PermissionError(self._get_logged_msg(f"Root or elavated mode required to start {self.APPLICATION_NAME}"))
        self._start()
        self.simulation_config["running"] = True
        if self._running_is_verifiable() and not self._is_running():
            raise BadStateError(self._get_logged_msg(f"Failed to start {self.APPLICATION_NAME}"))
        
    def stop(self):
        if self._install_is_verifiable() and not self._is_installed():
            raise BadStateError(self._get_logged_msg(f"{self.APPLICATION_NAME} not installed"))
        if self._running_is_verifiable() and not self._is_running():
            self._print(f"{self.APPLICATION_NAME} already stopped...")
            return
        if not Os.is_root_or_elevated_mode():
            raise PermissionError(self._get_logged_msg(f"Root or elavated mode required to stop {self.APPLICATION_NAME}"))
        self._stop() 
        self.simulation_config["running"] = False
        if self._running_is_verifiable() and self._is_running():
            raise BadStateError(self._get_logged_msg(f"Failed to stop {self.APPLICATION_NAME}")) 

    def _is_running(self):
        fake_response = None
        if self.os == OS_LINUX:
            if self.simulate_execution:
                print(self.simulation_config)
                fake_response = "active" if self.simulation_config["running"] else "inactive"
            response = self._execute(f"systemctl is-active {self.SERVICE_NAME}", return_response=True, fake_response=fake_response, success_codes=[0,3])
            if response.strip() == 'active':
                return True
            elif response.strip() == 'inactive':
                return False
        elif self.os == OS_WINDOWS:
            if self.simulate_execution:
                fake_response = "Status\nRunning " if self.simulation_config["running"] else "Status\nStopped "
            response = self._execute(f"powershell.exe Get-Service {self.SERVICE_NAME}", return_response=True, fake_response=fake_response)
            if "Running" in response:
                return True
            elif "Stopped" in response:
                return False            
        
  
class Docker(Daemon):

    APPLICATION_NAME          = "Docker"
    SERVICE_NAME              = "docker"
    DOCKER_USER_GROUP         = "docker"
    DEFAULT_DATA_ROOT_LINUX   = "/var/lib/docker" 
    DEFAULT_DATA_ROOT_WINDOWS = "C:/ProgramData/Docker"
    SYSCTL_SETTINGS           = ["vm.max_map_count=262144", "vm.swappiness=1"]
    SYSCTL_PATH               = "/etc/sysctl.conf"
    SUPPORTED_OS              = [OS_WINDOWS, OS_LINUX]
    SUPPORTED_DISTROS         = [DISTRO_CENTOS, DISTRO_UBUNTU]
    SERVICE                   = "service"
    SYSTEMCTL                 = "systemctl"
    SYSTEM_MANAGER            = SERVICE
    VIRTUAL_MEMORY_FILE       = "/var/run/docker.sock"
        
    def __init__(self, simulate_execution=False, simulation_config={}, silent=False, data_root=None):
        super().__init__(simulate_execution, simulation_config, silent)
        if data_root:
            self.data_root = data_root
        else:
            config = self._get_config_file_content()
            if "data-root" in config:
                self.data_root = config["data-root"]
            elif self.os == OS_LINUX:
                self.data_root = self.DEFAULT_DATA_ROOT_LINUX
            elif self.os == OS_WINDOWS:
                self.data_root = self.DEFAULT_DATA_ROOT_WINDOWS    
        repro_url_prefix = "https://download.docker.com/linux"
        self.gpg_url = None
        self.package_manager = None
        self.repo_url = None
        if not self.os in self.SUPPORTED_OS:
            raise NotSupported(self._get_logged_msg(f"'{self.os}' is not a supported OS"))             
        if self.os == OS_LINUX: 
            if not self.linux_distro in self.SUPPORTED_DISTROS:
                raise NotSupported(self._get_logged_msg(f"'{self.linux_distro}' is not a supported Linux Distro"))
            self.repo_url = f"{repro_url_prefix}/{self.linux_distro}"
            if self.linux_distro in DISTROS_USING_APT:
                self.package_manager = APT_MANAGER
            elif self.linux_distro in DISTROS_USING_YUM:
                self.package_manager = YUM_MANAGER
            else:
                raise NotImplementedError(self._get_logged_msg(f"Distro '{self.linux_distro}' is not supported"))
            self.gpg_url = f"{self.repo_url}/gpg"
            
    def _start(self): 
        self._print(f"Starting {self.APPLICATION_NAME}...")
        if self.os == OS_WINDOWS:    
            self._execute("powershell.exe Start-Service Docker")
        elif self.os == OS_LINUX:
            self._execute(self._get_commandline("start"))
                
    def _stop(self):
        self._print(f"Stopping {self.APPLICATION_NAME}...")
        if self.os == OS_WINDOWS:    
            self._execute("powershell.exe Stop-Service Docker")  
        elif self.os == OS_LINUX:
            self._execute(self._get_commandline("stop"))
            
    def _uninstall(self):
        if self.os == OS_WINDOWS:
             self._execute("powershell.exe Uninstall-package docker")
             self._execute("powershell.exe Uninstall-Module dockerprovider")
        elif self.package_manager == APT_MANAGER:
            self._execute(f"apt-get purge -y docker-engine docker docker.io docker-ce docker-ce-cli")
            self._execute(f"apt-get autoremove -y --purge docker-engine docker docker.io docker-ce docker-ce-cli")
        elif self.package_manager == YUM_MANAGER:
            self._execute("yum remove -y docker-ce")
            
    def _find_docker_root_dirs(self):
        if self.os == OS_LINUX:
            if not Os.is_root_or_elevated_mode():
                raise PermissionError("Root user mode required for finding Docker directories")
            paths = []
            for directory in ["volumes", "overlay2", "image", "containers", "network", "plugins", "builder"]:
                response = self._execute(f"find / -name {directory} -user root -print", return_response=True)  
                paths += ['/'.join(directory.split('/')[:-1]) for directory in response.split('\n') if directory.endswith(directory)]
            dir_paths = []
            for dir_path in set(paths):
                if len([path for path in paths if path == dir_path and path != '']) >= 4:
                    dir_paths.append(dir_path)
            return dir_paths
        return []
        
    def _user_approved_data_removal(self, dir_path):
        if exists(dir_path):
            while True:
                response = input(f"Do you want to delete everything under '{dir_path}'(y/n)? ")
                letter = response.lower()
                if letter in ['y', 'n']:
                    if letter == 'y':
                        self._print(f"Deleting all data under directory '{dir_path}'...")
                        #self._execute(f"rm -rf {dir_path}")
                        Os.del_dir(dir_path)
                    elif letter == 'n':
                        self._print(f"Skipping deletion of directory '{dir_path}'")
                    break
 
    def _possible_data_removal(self, detected_docker_root_dirs, possible_data_root, is_default=False): 
        if is_default:
            dir_type = "default"
        else:
            dir_type = "configured" 
        if possible_data_root in detected_docker_root_dirs:
            self._print(f"The {dir_type} Docker data directory detected at '{possible_data_root}'")
            self._user_approved_data_removal(possible_data_root)
            detected_docker_root_dirs.remove(possible_data_root)                 
        elif exists(possible_data_root):
            self._print(f"The {dir_type} Docker data directory at '{possible_data_root}' does not seem to be complete")
            self._user_approved_data_removal(possible_data_root)
        else:
            self._print(f"The {dir_type} Docker data directory '{possible_data_root}' does not exists")
        return detected_docker_root_dirs
                    
    def nuke_everything(self):
        if self._is_installed:
            self.uninstall()
        default_data_root = None
        configured_data_root = None
        config_file_path = self._get_config_file_path() 
        if exists(config_file_path):
            config = self._read_json_from_file(config_file_path, fake_content='{"data-root":self.data_root}')
            if "data-root" in config:
                configured_data_root = config["data-root"]
            else:
                self._print(f"No data-root configuration in '{config_file_path}'")
        else:
            self._print(f"No Docker configuration file to delete at '{config_file_path}'")
            default_data_root = self.data_root
        detected_docker_root_dirs = self._find_docker_root_dirs()
        if configured_data_root:
            detected_docker_root_dirs = self._possible_data_removal(detected_docker_root_dirs, configured_data_root, False)
        if default_data_root:
            detected_docker_root_dirs = self._possible_data_removal(detected_docker_root_dirs, default_data_root, True)
        if len(detected_docker_root_dirs):
            for directory in detected_docker_root_dirs:
                self._print(f"A possible Docker data directory not in the docker confifguration detected at '{directory}'")
                self._user_approved_data_removal(directory)
        if self.os == OS_LINUX:
            if Os.group_exists(self.DOCKER_USER_GROUP, self.simulate_execution, fake_response=True):
                self._print(f"Deleting user group 'docker'...")
                Os.delete_group(self.DOCKER_USER_GROUP, simulate=self.simulate_execution) 
            self._print(f"Deleting virtual memory file '{self.VIRTUAL_MEMORY_FILE}'...")
            self._execute(f"rm -rf {self.VIRTUAL_MEMORY_FILE}") 
            config_dir = self._get_config_file_dir_path()
            self._print(f"Deleting config directory '{config_dir}'...")
            self._execute(f"rm -rf {config_dir}")
            
    def _get_commandline(self, cmd):
        if self.SYSTEM_MANAGER == self.SERVICE:
            return f"{self.SYSTEM_MANAGER} docker {cmd}"
        elif self.SYSTEM_MANAGER == self.SYSTEMCTL:
            return f"{self.SYSTEM_MANAGER} {cmd} docker"
            
    def _prerequisites(self):
        self._print(f"Preparing to install {self.APPLICATION_NAME}...")   
        self._prepare_user()
        self._prepare_host()
        self._setup_repository()
        self._update_docker_config_file(self._gen_config(self.data_root, "overlay2"))
        
    def _post_install(self):
        self._enable_auto_start()
        self._enable_user()
        
    def _enable_user(self):
        if self.os == OS_LINUX:
            self._print("Enable user to run Docker command line tool...")
            path = "/home/{user}/.docker"
            user = Os.get_actual_user()
            Os.change_owner(path, user, user, True)
            Os.change_mode(path, 'g+rwx', True)

    def _update_sysctl_conf_file(self):
        settings = self.SYSCTL_SETTINGS
        content = self._read_from_file(self.SYSCTL_PATH, fake_content="vm.swappiness = 10\n\n# comment\nvm.dirty_ratio = 80")
        variables = [assignment.split('=')[0].strip() for assignment in self.SYSCTL_SETTINGS]
        lines = content.split('\n')
        for line in lines:
            keep_line = True
            for variable in variables:
                if line.startswith(variable):
                    keep_line = False
                    break
            if keep_line:
                settings.append(line)
        self._write_to_file(self.SYSCTL_PATH,'\n'.join(settings))
            
    def _prepare_host(self):
        if self.os == OS_LINUX:
            self._print("Preparing host...")
            self._update_sysctl_conf_file()
            self._execute("sysctl -p")
            
    def _prepare_user(self):
        if self.os == OS_LINUX:
            self._print("Preparing user...")
            formal_user = getuser()
            actual_user = Os.get_actual_user()
            if formal_user == "root" and actual_user and len(actual_user):
                user = actual_user
            else:
                user = formal_user
            log.info(f"Formal user: '{formal_user}', Actual user: '{actual_user}', User: '{user}'")
            Os.add_user_to_groups(user, "docker", self.simulate_execution)

    def _is_installed(self):
        fake_response = None
        if self.simulate_execution:
            if self.simulation_config["installed"]:
                fake_response = "Docker version 19.03.12, build 48a66213fe"
            elif self.simulation_config["os"] == OS_LINUX:
                fake_response = "\nCommand 'docker' not found, but can be installed with:\n\nsudo snap install docker"
            else:
                fake_response = "'docker' is not recognized as an internal or external command,\noperable program or batch file."
        response = self._execute("docker -v", return_response=True, ignore_error=True, fake_response=fake_response)
        if response.startswith("Docker version"):
            return True
        return False 
       
    def _update_package_index(self):
        if self.package_manager == APT_MANAGER:
            self._execute("apt-get update")
        elif self.package_manager == YUM_MANAGER:
            self._execute("yum -y update")
            
    def _install_repository_tools(self):
        if self.package_manager == APT_MANAGER:
            self._execute("apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common --assume-yes")  
        elif self.package_manager == YUM_MANAGER:
            self._execute("yum install -y yum-utils device-mapper-persistent-data lvm2") 
            
    def _add_gpg_key(self):
        if self.package_manager == APT_MANAGER:
            self._execute(f"curl -fsSL {self.gpg_url} | apt-key add -")
            self._execute("apt-key fingerprint 0EBFCD88")  
        
    def _add_repository(self):
        if self.package_manager == APT_MANAGER:
            self._execute(f'add-apt-repository "deb [arch=amd64] {self.repo_url} $(lsb_release -cs) stable"')
        elif self.package_manager == YUM_MANAGER:
            self._execute(f"yum-config-manager --add-repo {self.repo_url}/docker-ce.repo")
        
    def _get_latest_available_version(self):
        version_string = None
        fake_response = "docker-ce | 5:19.03.12~3-0~ubuntu-bionic | https://download.docker.com/linux/ubuntu bionic/stable amd64 Packages"
        response = self._execute("apt-cache madison docker-ce", return_response=True, fake_response=fake_response)
        log.debug(response)
        for line in response.split("\n"):
            regex = f"^docker-ce \| (.*) \| {self.repo_url} bionic/stable amd64 Packages$"
            m = match(regex, line.strip())
            if m:
                version_string = m.group(1)
                break
        return version_string
        
    def _setup_repository(self):
        if self.os == OS_LINUX:
            self._print("Setting up Docker repository...")
            self._update_package_index()
            self._install_repository_tools()
            self._add_gpg_key()
            self._add_repository()
            
    def _update_docker_config_file(self, settings):
        config = self._get_config_file_content()
        for key in settings.keys():
            config[key] = settings[key]
        if config:
            dir_path = self._get_config_file_dir_path()
            if not exists(dir_path):
                self._make_dir(dir_path)
            self._pretty_write_to_file(self._get_config_file_path(), config)
                
    def _get_config_file_path(self):
        return join(self._get_config_file_dir_path(), "daemon.json")
        
    def _get_config_file_content(self):
        return self._read_json_from_file(self._get_config_file_path(), fake_content='{"debug": false}')     
         
    def _get_config_file_dir_path(self):
        if self.os == OS_LINUX: 
            return "/etc/docker"
        elif self.os == OS_WINDOWS:
            if self.simulate_execution:
                return join("C:/Programdata/docker/config")
            else:
                return join(environ['programdata'], "docker", "config")
        raise NotSupported("'{self.os}' is not a supported OS")
            
    def _gen_config(self, data_root=None, storage_driver=None):
        config = {}
        if data_root:
            config["data-root"] = data_root
        if storage_driver:
            config["storage-driver"] = storage_driver
        return config

    def _install(self):
        self._print("Installing Docker...")
        if self.os == OS_WINDOWS:       
            #self._execute("powershell.exe Find-PackageProvider -Name 'Nuget' -ForceBootstrap -IncludeDependencies")
            self._execute("powershell.exe Install-PackageProvider -Name NuGet -Force")
            self._execute("powershell.exe Install-Module DockerMsftProvider -Force")
            self._execute("powershell.exe Install-Package Docker -ProviderName DockerMsftProvider -Force -RequiredVersion 19.03")
            self.requires_restart = True
        elif self.package_manager == APT_MANAGER:
            version = self._get_latest_available_version()
            log.debug(f"Latest available docker version is '{version}'")
            if version:
                self._execute(f"apt-get install docker-ce={version} docker-ce-cli={version} containerd.io --assume-yes")
            else:
                self._execute("apt-get install docker-ce docker-ce-cli containerd.io --assume-yes") 
        elif self.package_manager == YUM_MANAGER:
            self._execute("yum install -y docker-ce docker-ce-cli containerd.io")
                  
    def _enable_auto_start(self): 
        if self.os == OS_LINUX:
            self._print("Enabling auto start...") 
            self._execute("systemctl enable docker")


class DockerCompose(Application): 

    APPLICATION_NAME = "Docker-Compose"
    
    def install(self):
        if self._is_installed():
            self._print(f"{self.APPLICATION_NAME} already installed")
        else:
            if self.os == OS_WINDOWS:
                choco = Choco(self.simulate_execution, self.original_simulation_config)
                choco.install()
                if choco.is_requiring_restart():
                    self._restart_server()
            self._install()
        
    def uninstall(self):
        if not self._is_installed():
            self._print(f"{self.APPLICATION_NAME} not installed")
            return
        self._print(f"Uninstalling {self.APPLICATION_NAME}...")
        if self.os == OS_WINDOWS:  
            raise NotImplementedError
        elif self.os == OS_LINUX:
            self._execute("rm $(which docker-compose)")  

    def _is_installed(self):
        fake_response = None
        if self.simulate_execution:
            if self.simulation_config["installed"]:
                fake_response = "docker-compose version 1.24.1, build 4667896b"
            else:
                fake_response = "'docker-compose' is not recognized"
        response = self._execute("docker-compose -v", return_response=True, ignore_error=True, fake_response=fake_response)
        if response.startswith("docker-compose version"):
            return True
        return False
        
    def _prerequisites(self):
        Choco(self.simulate_execution, self.original_simulation_config).install()

    def _install(self, version=None): 
        if self.os == OS_WINDOWS:  
            self._execute("choco install docker-compose -y")
        elif self.os == OS_LINUX:
            ver = "1.24.0"
            if version:
                ver = version
            link = f"https://github.com/docker/compose/releases/download/{ver}/docker-compose-$(uname -s)-$(uname -m)"
            self._execute(f'curl -L "{link}" -o {DOCKER_COMPOSE_EXE}')          


class Choco(Application):

    APPLICATION_NAME = "Choco"
            
    def _is_installed(self):
        fake_response = None
        if self.simulate_execution:
            if self.simulation_config["installed"]:
                fake_response = "0.10.15"
            else:
                fake_response = "'choco' is not recognized"
        response = self._execute("choco -v", return_response=True, ignore_error=True, fake_response=fake_response)
        if response.startswith("0.") or response.startswith("1."):
            return True
        return False   
         
    def install(self):
        if self.os == OS_WINDOWS:
            if self._is_installed():
                self._print("Choco already installed")
            else:
                self._print("Installing Choco...")
                self._execute("powershell.exe Set-ExecutionPolicy Bypass -Scope Process -Force")
                self._execute("powershell.exe Invoke-Expression((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1'))")
                self.requires_restart = True


class DockerComposeApp(Daemon):

    def __init__(self, install_dir, docker_credentials, simulate_execution=False, simulation_config={}, silent=False):
        super().__init__(simulate_execution, simulation_config, silent)
        self.install_dir = install_dir
        self.docker_credentials = docker_credentials

    def start(self):
        self._docker_login()
        with change_dir(self.install_dir):
            self._print(f"Starting {self.APPLICATION_NAME} (this may take many minutes the first time)...")
            try:
                self._execute("docker-compose up -d")
            except:
                raise
        
    def stop(self):
        if self._is_running():
            with change_dir(self.install_dir):
                self._print(f"Stopping {self.APPLICATION_NAME}...")
                self._execute("docker-compose down")
        
    def _is_running(self):
        docker_compose_running = "NameCommandStatePorts"
        inspector_running = "_api_"
        fake_response = None
        if self.simulate_execution:
            fake_response = docker_compose_running + inspector_running if self.simulation_config["running"] else 'bad response'
        with change_dir(self.install_dir):
            response = self._execute(f"docker-compose ps", return_response=True, fake_response=fake_response, success_codes=[0,3])
            response = (' '.join([line.replace(' ', '').replace('\t', '') for line in response.split('\n')]))
            if docker_compose_running in response and inspector_running in response:
                return True
        return False
                
    def _docker_login(self):
        if self.docker_registry and self.docker_credentials:
            user = self.docker_credentials["user"]
            password = PWD_START_STOP_TOKEN + docker_credentials["password"] + PWD_START_STOP_TOKEN
            self._execute(f'docker login -u "{user}" -p "{password}" {self.docker_registry}') 

    
class InspectorDockerApp(DockerComposeApp):   

    APPLICATION_NAME       = "ayfie Inspector"
    LINUX_DOCKER_REGISTRY  = "quay.io" 
    WIN_DOCKER_REGISTRY    = "index.docker.io"
    INSPECTOR_BASH_SCRIPTS = ["start-ayfie.sh", "stop-ayfie.sh", "ayfie-logs.sh", "create_certs.sh"] 
    SECURITY_ASSETS_DIR    = "security-assets"   
    DOCKER_COMPOSE_EXE     = "/usr/bin/docker-compose"
        
    def __init__(self, installer_url=None, download_dir=None, install_dir=None, docker_credentials=None, docker_compose_yml_files=[], 
                       simulate_execution=False, simulation_config={}, silent=False, files_to_copy=[], dot_env_file_path=None, 
                       self_sign_certificate={}, version=None, post_install_user=None, docker_data_dir=None):
        super().__init__(install_dir, docker_credentials, simulate_execution, simulation_config, silent)
        self.installer_url = installer_url
        if self.os == OS_LINUX:
            self.docker_registry = self.LINUX_DOCKER_REGISTRY
        elif self.os == OS_WINDOWS:
            self.docker_registry = self.WIN_DOCKER_REGISTRY 
        self.download_dir = download_dir
        self.install_dir = install_dir
        self.docker_compose_yml_files = docker_compose_yml_files
        self.files_to_copy = files_to_copy
        self.dot_env_file_path = dot_env_file_path
        self.self_sign_certificate = self_sign_certificate
        self.version = version
        self.post_install_user = post_install_user
        self.docker_data_dir = docker_data_dir
        
    def _add_custom_files(self, custom_files):
        files_to_copy = []
        for from_file in custom_files:
            destination_file = join(self.install_dir, from_file)
            if exists(destination_file):
                continue
            if exists(from_file):
                files_to_copy.append((from_file, destination_file))
                copy(from_file, destination_file)
            else:
                raise FileNotFoundError(f"Could not find '{from_file}' or '{destination_file}'")
        if files_to_copy:
            files = "', '".join([file[0] for file in files_to_copy])
            self._print(f"Copying in custom file(s): {files}...")
            for from_file, destination_file in files_to_copy:
                copy(from_file, destination_file)
            
    def _prerequisites(self):
        docker = Docker(self.simulate_execution, self.original_simulation_config, data_root=self.docker_data_dir)
        docker.install()
        if not docker.is_requiring_restart():   
            docker.start()
        DockerCompose(self.simulate_execution, self.original_simulation_config).install()       
        
    def _install_is_verifiable(self):
        return False 

    def _install_is_runnable(self):
        return False        

    def _install(self): 
        if not self.download_dir or not self.install_dir:
            raise NotEnoughInformation("download and/or install directory not set")
        self._print("Downloading and unzipping the ayfie Inspector installer...")
        zipped_installer = WebContent.download(self.installer_url, self.download_dir)
        if not FileTools.unzip(zipped_installer, self.install_dir):  
            raise BadZipFileError(f'File "{zipped_installer}" is not a zip file')
        if self.files_to_copy:
            self._print("Copying in custom files...")
            for file_to_copy in self.files_to_copy:
                copy(file_to_copy, self.install_dir)

    def _uninstall(self): 
        Os.del_dir(self.install_dir) 
        
    def _prune_system(self): 
        execute("docker system prune --volumes --force", timeout=60)
        
    def nuke_everything(self):  
        docker = Docker(self.simulate_execution, self.original_simulation_config, data_root=self.docker_data_dir)
        docker.stop()
        docker.nuke_everything()
         
    def _customize(self):
        custom_files = []
        for custom_file in self.docker_compose_yml_files:
            custom_files.append(custom_file)
        if self.dot_env_file_path:
            custom_files.append(self.dot_env_file_path)
        self._add_custom_files(custom_files)
        
    def _get_file_pair(self, template_file_name):
        return (template_file_name, template_file_name.replace("-template", ""))
    
    def _configure_gatekeeper(self):
        if self.self_sign_certificate: 
            self._print("Configuring Gatekeeper...")
            if is_earlier_version(self.version, "2.8.1"):
                file_pairs = [
                    self._get_file_pair(CONFIG_TEMPLATE)        
                ]
            elif is_earlier_version(self.version, "2.9.0"):
                file_pairs = [
                    self._get_file_pair(FRONTEND_CONFIG_TEMPLATE),
                    self._get_file_pair(SECURE_API_CONFIG_TEMPLATE)
                ]
            else:
                return
            with change_dir(join(self.install_dir, SECURITY_ASSETS_DIR)):  
                for pair in file_pairs:
                    from_file = pair[0]
                    to_file = pair[1]
                    self._execute(f"cp {from_file} {to_file}")
                    content = self._read_from_file(to_file)
                    content = content.replace("auth.localhost", self.self_sign_certificate['domain'])
                    self._write_to_file(to_file, content)

    def _create_self_sign_certificate(self):
        self._print("Creating self sign certificate...")
        x = self.self_sign_certificate
        with change_dir(join(self.install_dir, SECURITY_ASSETS_DIR)):
            self._execute(f"./create_certs.sh -c {x['country']} -st {x['state']} -l {x['city']} -o {x['company']} -cn {x['domain']}") 
                      
    def _prepare_security(self):
        if self.self_sign_certificate:
            self._create_self_sign_certificate()
        security_assets_dir = join(self.install_dir, self.SECURITY_ASSETS_DIR)
                    
    def make_scripts_executable(self):
        if self.os == OS_LINUX:
            self._print("Making scripts executable...")
            for directory in [self.install_dir, join(self.install_dir, self.SECURITY_ASSETS_DIR)]:
                if exists(directory):
                    with change_dir(directory):
                        for item in self.INSPECTOR_BASH_SCRIPTS:
                            if exists(item):
                                self._execute(f"chmod +x {item}") 
            if exists(self.DOCKER_COMPOSE_EXE):
                fake_response = "-rw-r--r-- 1 root root 16154160 Aug 14 03:10 /usr/bin/docker-compose"
                file_listing = self._execute(f"ls -l {self.DOCKER_COMPOSE_EXE}", return_response=True, fake_response=fake_response)
                if file_listing.split()[0].count('x') !=  3:
                    self._execute(f"chmod +x {self.DOCKER_COMPOSE_EXE}")
                
    def _post_install(self): 
        self._prepare_security()
        self.make_scripts_executable()
        self._customize()
        self._configure_gatekeeper()
        user = Os.get_actual_user()
        user_docker_dir = f"/home/{user}/.docker"        
        Os.change_owner(user_docker_dir, user, user, True) 
        Os.change_mode(user_docker_dir, "g+rwx", True)
        docker_file = join(user_docker_dir, "config.json")   
        Os.change_owner(docker_file, user, user) 


class Installation(EngineConnector):

    def _post_init(self):
        if self.config.installer or self.config.uninstaller:
            try:
                self.version = self._extract_version_number()
            except ValueError:
                self.version = None
            self.host_os = HOST_OS
            if not self.host_os in [OS_WINDOWS, OS_LINUX]:
                raise OSError(f"'{os}' is not a supported OS")
            if self.config.install_dir:
                self.install_dir = self.config.install_dir
            else:
                self.install_dir = self.version
            if self.host_os == OS_LINUX:
                self.docker_registry = LINUX_DOCKER_REGISTRY
            elif self.host_os == OS_WINDOWS:
                self.docker_registry = WIN_DOCKER_REGISTRY
            self.inspector_instance = self._get_inspector_instance()
            
    def _extract_version_number(self):
        if self.config.version:
            return self.config.version
        err_msg = "Version number not provided"
        if self.config.installer_zip_file:
            m = match("^.*([1-2]\.[0-9]+.[0-9]+).*$", self.config.installer_zip_file)
            if m:
                return m.group(1)
            err_msg += " and unable to extract the version from the installer url."
        else:
            err_msg += " and also no installer url to extract the version info from."
        raise ValueError(err_msg)
        
    def _double_report(self, message):
        log.info(message)
        print(message) 

    def operate(self):
        if self.config.stop or self.config.uninstall or self.config.nuke_everything:
            self.inspector_instance.stop()
        if self.config.uninstall or self.config.nuke_everything:
            self.inspector_instance.uninstall()
        if self.config.nuke_everything:
            self.inspector_instance.nuke_everything()
            
            
class Uninstaller(Installation):

    def _get_inspector_instance(self):
        return InspectorDockerApp(
            install_dir=self.install_dir, 
            simulate_execution=self.config.simulation,
            simulation_config={},  
            version=self.version,
            docker_data_dir=self.config.docker_data_dir
        )


class Installer(Installation): 

    def _get_inspector_instance(self):
        return InspectorDockerApp(
            installer_url=self._get_installer_url(),
            download_dir='.', 
            install_dir=self.install_dir,
            docker_credentials=self.config.docker_registry, 
            docker_compose_yml_files=[],
            simulate_execution=self.config.simulation,
            simulation_config={}, 
            #files_to_copy=self.config.copy_to_install_dir,
            #dot_env_file_path=dot_env_file_path,                        
            #self_sign_certificate=self.self_sign_certificate, 
            version=self.version,
            post_install_user=None,
            docker_data_dir=self.config.docker_data_dir
        )
                    
    def _disable_defender(self):
        if self.host_os == OS_WINDOWS:
            if self.config.defender_exclude_paths:
                for path in self.config.defender_exclude_paths:
                    msg = f"Windows Defender realtime scanning disabled for directory '{path}'"
                    if self.config.simulation:
                        msg = f"Simulating: {msg}"
                    else:
                        execute(f'powershell -Command Add-MpPreference -ExclusionPath "{path}"')
                    print(msg)
            else:              
                msg = "Windows Defender realtime scanning disabled"
                if self.config.simulation:
                    msg = f"Simulating: {msg}"
                else:
                    execute(f'powershell -Command Set-MpPreference -DisableRealtimeMonitoring $true')
                print(msg)
                
    def _gen_application_custom_file(self):
        yaml_version = self._get_docker_compose_format_version()
        applicationYamlFile = ApplicationYamlFile(self.install_dir, yaml_version, self.host_os)
        if self.config.enable_gdpr:
            applicationYamlFile.enable_gdpr() 
        if self.config.enable_alt_threading:
            applicationYamlFile.enable_email_treading()
        if self.config.enable_admin_page and self.config.enable_frontend:
            applicationYamlFile.enable_admin_page()
        applicationYamlFile.generate_file()

    def _wait_until_engine_ready(self, timeout=None):
        waited_so_far = 0
        pull_interval = 1
        while not self.engine.engine_is_healthy():
            if timeout and waited_so_far > timeout:
                raise TimeoutError("Giving up waiting for engine to become ready")
            sleep(pull_interval)
            waited_so_far += pull_interval       

    def operate(self):
        app_down = True
        if self.config.install:
            if self.config.disable_defender:
                self._disable_defender()
            self.inspector_instance.install()
            self._gen_application_custom_file()  
            self.docker_env_file = DockerEnvFile(self.install_dir, self.version, self.config, os=self.host_os, filename=DOCKER_DOT_ENV_FILE)
            self.docker_env_file.generate_file()
        if self.config.start: 
            self._double_report("Starting the Inspector - downloading any required Docker images not already downloaded. This could take many minutes...")        
            self.start_inspector()
            self._double_report("Docker image download (if any) completed. Now waiting for Elasticsearch to come up...")
            if self.config.security:
                input("Manual Keyecloak operations are now required. CLICK ENTER WHEN READY TO CONTINUE.")
            self._wait_until_engine_ready()
            self._double_report("The ayfie Inspector is now up and ready")
            app_down = False
        if app_down:
            raise EngineDown
  
    def _get_concluded_os(self):
        if not self.config.os in SUPPORTED_OS_VALUES:
            raise ValueError("f{self.config.os} is not a supported OS")
        if self.config.os:
            if self.config.os == OS_AUTO:
                if self.config.installer_zip_file:
                    m = match(f"^.*ayfie-({OS_WINDOWS}|)(|-)installer.*.zip$", self.config.installer_zip_file)
                    if m:
                        if m.group(1) == "":
                            return OS_LINUX
                        return m.group(1)
                elif self.host_os in [OS_WINDOWS, OS_LINUX]:
                    return self.host_os
                else:
                    raise OSError(f"'{self.host_os}' is not a supported OS'")
            else:
                return self.config.os
        raise AutoDetectionError("Unable to obtain OS")

    def _add_docker_compose_yml_file(self, enable_feature, custom_file):       
        if enable_feature:
            if not custom_file in self.docker_compose_custom_files:
                self.docker_compose_custom_files.append(custom_file)
                 
    def _inspector_operation(self, operation):
        if not self.install_dir:
            if self.config.version:
                print("No install directory given, will try with version number as directory name") 
                self.install_dir = self.config.version
            else:
                raise NotEnoughInformation("Do not know and are unable to guess where ayfie Inspector is installed")
        with change_dir(self.install_dir):
            script = f"{operation}-ayfie.{Os.get_script_extension()}"
            result = execute(join(".", script), return_response=True).lower()
            if "error" in result:
                if "bind: address already in use" in result:
                    raise ConfigError("The selected port is already being used by some other application. Check the logs for more details.")
                else:
                    print(f"WARNING: The output from '{script}' seems to contain an error message. Check the logs for details.")

    def _do_docker_login(self):
        if not self.config.docker_registry:
            log.warning("No docker registry user credentials")
            return
        if len(self.config.docker_registry) == 1:
            if not "os" in self.config.docker_registry[0]:
                self.config.docker_registry[0]["os"] = self._get_concluded_os()
        elif len(self.config.docker_registry) > 1: 
            for docker_registry in self.config.docker_registry:
                if not "os" in docker_registry:
                    raise ValueError(f"If credentials for more than one docker regitry is given, then the OS has to also be given")
        for docker_registry in self.config.docker_registry:
            for item in ["user", "password", "os"]:
                if not item in docker_registry:
                    raise ValueError(f"Bad account data: {str(docker_registry)}")
            if docker_registry["os"] == self._get_concluded_os():
                user = docker_registry["user"]
                password = PWD_START_STOP_TOKEN + docker_registry["password"] + PWD_START_STOP_TOKEN
                
                if self.config.simulation:
                    print(f'docker login -u "{user}" -p "{password}" {self.docker_registry}')
                else:
                    execute(f'docker login -u "{user}" -p "{password}" {self.docker_registry}')
                break
        else:
            log.warning("No docker registry user credentials")
                     
    def start_inspector(self): 
        self._do_docker_login()      
        self._inspector_operation("start")
        
    def _get_mem_limit(self, limit_64_ram, limit_128_ram, ram_on_host):
        import numpy
        coefs, _, _, _, _ = numpy.polyfit([64, 128], [limit_64_ram, limit_128_ram], deg=1, full=True)
        mem_limit = int(round(ram_on_host * coefs[0] + coefs[1]))
        if mem_limit < self.minimum_ram_allocation:
            mem_limit = self.minimum_ram_allocation
        return mem_limit

    def _gen_mem_limits(self):
        main_version = self.version.split(".")[0]
        if not main_version in ["1", "2"]:
            raise ValueError(f"There is no main version number '{str(main_version)}' of the ayfie Inspector")
        self.minimum_ram_allocation = 1
        freed_up_ram = 0
        for variable, limit_64_ram, limit_128_ram in MEM_LIMITS_64_AND_128_GB_RAM[main_version]:
            if variable == "SUGGEST_MEM_LIMIT" and self.config.no_suggest_index:
                freed_up_ram += self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host)
                freed_up_ram -= self.minimum_ram_allocation
            if variable == "ELASTICSEARCH_MEM_LIMIT":
                mem_limit = self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host)
                if mem_limit > ELASTICSEARCH_MAX_MEM_LIMIT:
                    freed_up_ram += mem_limit - ELASTICSEARCH_MAX_MEM_LIMIT
        mem_limits = []
        for variable, limit_64_ram, limit_128_ram in MEM_LIMITS_64_AND_128_GB_RAM[main_version]:
            if self.config.no_suggest_index and "SUGGEST" in variable:
                mem_limit = self.minimum_ram_allocation
            else:
                mem_limit = self._get_mem_limit(limit_64_ram, limit_128_ram, self.config.ram_on_host + freed_up_ram)
                if variable == "ELASTICSEARCH_MEM_LIMIT" and mem_limit > ELASTICSEARCH_MAX_MEM_LIMIT:
                    mem_limit = ELASTICSEARCH_MAX_MEM_LIMIT
                if variable == "ELASTICSEARCH_MX_HEAP" and mem_limit > ELASTICSEARCH_MAX_MX_HEAP:
                    mem_limit = ELASTICSEARCH_MAX_MX_HEAP
            self.docker_env_file.add_variable(variable, f"{mem_limit}G")
        
    def _retrieve_mem_limits_from_config(self, config_file_path):
        settings = []
        for line in open(config_file_path, "r").read().split('\n'):
            m = match("^.*- (.+)=\$\{.+:-(.+G)|^.*mem_limit: \$\{(.+):-(.+G)\}", line)
            if m:
                settings.append('='.join([x for x in m.groups() if x]))
        return list(set(settings))
        
    def _get_installer_url(self):
        if self.config.installer_zip_file:
            return self.config.installer_zip_file
        if not self.version:
            raise NotEnoughInformation("Needs Inspector installer zip file path or version number")
        concluded_os = self._get_concluded_os()
        zip_file_name = "ayfie-"
        if is_earlier_version(self.version, "2.12.0"):
            if concluded_os == OS_WINDOWS:
                zip_file_name += f"{concluded_os}-"
        else:
            zip_file_name += f"oem-{concluded_os}-"
        zip_file_name += f"installer-{self.version}.zip"
        return f"http://docs.ayfie.com/ayfie-platform/release/{zip_file_name}"
        
    def _get_docker_compose_format_version(self):
        os = self._get_concluded_os()
        if os == OS_LINUX:
            version_that_format_version_changed = "2.8.1"
        elif os == OS_WINDOWS:
            version_that_format_version_changed = "2.8.0"
        else:
            raise ValueError(f"'{os}' is not a supported OS")
        if is_earlier_version(self.version, version_that_format_version_changed):
            return "2.1"
        else:
            return "2.4"
          

class Operational(EngineConnector):

    """
            if operation == OP_FILE_TRANSFER:
                self._file_transfer(self.config.file_transfer)
    """
                 
    def _file_transfer(self, transfer_config):
        for config in transfer_config:
            try:
                protocol = config["protocol"]
                if protocol == "disabled":
                    continue
                server = config["server"].strip('/')
                local_dir = config["local_dir"]
                direction = config["direction"]
                if not direction in ["to_remote", "from_remote"]:
                    raise ValueError(f"File transfer config parameter 'direction' cannot have the value '{direction}'")
                if protocol in ['http', 'https']:
                    path = config["path"].strip('/')
                    if "port" in config:
                        port = config["port"]
                    else:
                        port = 80
                    filename = config["filename"]
                elif protocol in ['ssh']:
                    user = config["user"]
                    remote_dir = config["remote_dir"]
                    filename_pattern = config["filename_pattern"]
                else:
                    raise ValueError(f"Protocol '{protocol}' is not a supported protocol")
            except KeyError as e:
                raise KeyError(f"Key error for key {str(e)} in file transfer config: '{config}'")
              
            if protocol.startswith('http'):
                if direction == "to_remote":
                    raise NotImplementedError
                elif direction == "from_remote":
                    url_path = f"{protocol}://{server}:{port}/{path}/"
                    for filename in listify(filename):
                        url = url_path + filename
                        WebContent.download(url, local_dir)
            elif protocol == 'ssh':
                remote_server = RemoteServer(server, user)
                if direction == "from_remote":
                    file_paths = remote_server.find_files(remote_dir, filename_pattern)
                elif direction == "to_remote":
                    raise NotImplementedError("Copying from local to remote server has not yet been implemented")
                    if self.host_os == OS_WINDOWS:
                        file_paths = []
                    else:
                        file_paths = []
                for file_path in file_paths:
                    local_path = local_dir + "/" + basename(file_path)
                    remote_path = remote_dir + "/" + basename(file_path)
                    if direction == "from_remote":
                        print(f"Copying '{remote_path}' froms server '{server}' to local host as '{local_path}'")
                        remote_server.copy_file_from_remote(remote_path, local_path)
                    else:
                        print(f"Copying '{local_path}' to server '{server}' to as '{remote_path}'")
                        remote_server.copy_file_to_remote(local_path, remote_path)
 
class ResourceAnalyzer(): 

    def get_resource_utilization(self, numbers_with_units=True, space_between=True):
        install_python_modules('psutil')
        import psutil
        statistics = {
            "total_virtual_memory":     psutil.virtual_memory().total,
            "available_virtual_memory": psutil.virtual_memory().available, 
            "used_virtual_memory":      psutil.virtual_memory().used,
            "total_swap_memory":        psutil.swap_memory().total,
            "available_swap_memory":    psutil.swap_memory().free, 
            "used_swap_memory":         psutil.swap_memory().used,            
            "total_disk":               psutil.disk_usage('/').total,
            "used_disk":                psutil.disk_usage('/').used,
            "free_disk":                psutil.disk_usage('/').free,
        }
        space = ""
        if space_between:
            space = " "
        if numbers_with_units:
            for item in statistics:
                number, unit = get_unit_adapted_byte_figure(statistics[item])
                statistics[item] = f"{number}{space}{unit}"
        return statistics
        
    def get_disk_utilizations(self):
        install_python_modules('psutil')
        import psutil     
        return psutil.disk_partitions(all=True)
        
    def get_docker_info(self):  
        try:
            docker_info = execute("docker info --format '{{json .}}'", return_response=True)
            
            print(docker_info)
            print(type(docker_info))
            struct = loads(docker_info)
            print(struct)
        except CommandLineError as e: 
            if "docker" in str(e) and "not recognized" in str(e):
                return "Docker does not seem to be installed on this host"
            raise
        return docker_info

 
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

    def _init_class_global_variables(self):
        self.search_operation = False
        if self.config.doc_retrieval_always_use_search:
            self.search_operation = True
        elif self.config.doc_retrieval_query or self.config.doc_retrieval_filters:
            self.search_operation = True
        self.col_id = self.engine.get_collection_id(self.config.col_name)
        self.schema_fields = self._get_schema_fields()
        self.requested_fields = self._get_fields_to_extract()
        self.exclude_fields = self._get_fields_to_exclude(self.schema_fields, self.requested_fields)
        self.batch_size = self._get_batch_size(self.config.doc_retrieval_batch_size, self.exclude_fields)
        self.csv_output_columns = self.requested_fields[:]
        if self.config.doc_retrieval_field_merging:
            for new_field in self.config.doc_retrieval_field_merging:
                self.csv_output_columns.append(new_field)
                for old_field in self.config.doc_retrieval_field_merging[new_field]:
                    self.csv_output_columns.remove(old_field)
        
    def _get_batch_size(self, batch_size, exclude_fields): 
        if batch_size == "auto":
            batch_size = 1000
            if self.search_operation:
                if "content" in exclude_fields:
                    batch_size = MAX_PAGE_SIZE
        else:
            try:
                batch_size = int(batch_size)
            except:
                raise ValueError('Parameter "batch_size" must be an integer')
        if batch_size > MAX_PAGE_SIZE:
            batch_size = MAX_PAGE_SIZE
        return batch_size
     
    def merge_columns(self, row):
        if self.config.doc_retrieval_field_merging:
            for new_field in self.config.doc_retrieval_field_merging:
                new_column = []
                for old_field in self.config.doc_retrieval_field_merging[new_field]:
                    if old_field in row:
                        if len(row[old_field]):
                            new_column.append(row[old_field])
                        del row[old_field]
                row[new_field] = self.config.doc_retrieval_separator.join(new_column)
        return row
        
    def retrieve_documents(self):
        self._init_class_global_variables()  
        with open(self.config.doc_retrieval_output_file, mode='w', newline='', encoding="utf-8") as f:
            csv_writer = csv.DictWriter(f, fieldnames=self.csv_output_columns, dialect=self.config.doc_retrieval_csv_dialect)
            if self.config.doc_retrieval_csv_header_row:
                csv_writer.writeheader()
            doc_count = 0
            empty_doc_count = 0
            empty_field_count = 0
            field_content_count = 0
            skip_message = "."
            for doc_batch in self._get_documents(self.config.doc_retrieval_query, self.config.doc_retrieval_filters):
                for document in doc_batch:
                    if self.config.doc_retrieval_max_docs:
                        if doc_count >= self.config.doc_retrieval_max_docs:
                            self._print(f"Aborting after having extracted {doc_count} documents as specified by parameter 'max_docs'{skip_message}")
                            return
                    row = {}
                    doc_is_empty = True
                    doc_has_empty_field = False
                    doc_has_field_content = False
                    for field in self.requested_fields:
                        if self.search_operation:
                            value = document["document"].get(field, '')
                        else:
                            value = document.get(field, '')
                        if type(value) is int:
                            value = str(value)
                        if field in self.config.doc_retrieval_skip_if_field_empty:
                            if value == '':
                                doc_has_empty_field = True
                        if field in self.config.doc_retrieval_skip_if_field_content:
                            if value != '':
                                doc_has_field_content = True
                        if type(value) is list:
                            value = self.config.doc_retrieval_separator.join(value)
                        if type(value) is str:
                            value = value.replace('\n', ' ').replace('\t', ' ')
                        if len(value):
                            if field != "id":
                                doc_is_empty = False
                        row[field] = value
                    row = self.merge_columns(row)
                    if doc_is_empty and self.config.doc_retrieval_skip_empty_documents:
                        empty_doc_count += 1
                    elif doc_has_empty_field and self.config.doc_retrieval_skip_if_field_empty:
                        empty_field_count += 1
                    elif doc_has_field_content and self.config.doc_retrieval_skip_if_field_content:
                        field_content_count += 1
                    else:
                        csv_writer.writerow(row)
                    skip_count = empty_doc_count + empty_field_count + field_content_count
                    if skip_count:
                        skip_message = f" of which {skip_count} documents were skipped for these reasons: empty ({empty_doc_count}), "
                        skip_message += f"missing required fields ({empty_field_count}), having unwanted fields ({field_content_count})."
                    if doc_count & (doc_count % self.config.doc_retrieval_report_frequency == 0):
                        self._print(f"So far {doc_count} documents have been extracted{skip_message}")
                    doc_count += 1
        self._print(f"Done after having extracted {doc_count} documents{skip_message}")
                     
    def _get_documents(self, query="", filters=[], exclude_fields=[]):
        if self.search_operation:
            result_page = self.engine.search_collection(self.col_id, query, self.batch_size, 0, filters,
                                                        self.exclude_fields, scroll=True, meta_data=True)
            yield result_page["result"]
        else:
            result_page = self.engine.get_collection_documents(self.col_id, self.batch_size)
            yield result_page["_embedded"]["documents"]
        for result_page in self.engine.get_next_scroll_pages(result_page):
            if self.search_operation:
                yield result_page["result"]
            else:  
                yield result_page["_embedded"]["documents"]
                          
    def _get_schema_fields(self):
        schema = self.engine.get_collection_schema(self.col_id)
        if "fields" in schema:
            return [field for field in schema["fields"].keys()]
        raise KeyError(f"No field 'fields' in schema: {schema}")
 
    def _get_fields_to_extract(self):
        fields = self.config.doc_retrieval_fields
        if len(fields) == 1:
            if ',' in fields[0]:
                fields = fields[0].split(',')
        if not fields:
            fields = self.schema_fields
        if self.config.doc_retrieval_drop_underscore_fields:
            fields = [field for field in fields if not field.startswith("_")]
        if not "id" in fields:
            fields = ["id"] + fields
        for field in self.config.doc_retrieval_exclude:
            try:
                fields.remove(field)
            except ValueError:
                raise ValueError(f"Exclude list contains a non-existing field '{field}'. Check the spelling.")
        return fields
        
    def _get_fields_to_exclude(self, schema_fields, requested_fields):
        return [field for field in schema_fields if field not in requested_fields]
        
            
class Feeder(EngineConnector):

    def collection_exists(self):
        if self.config.col_name:
            if self.engine.exists_collection_with_name(self.config.col_name):
                return True
            else:
                return False
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
        if not self.config.col_name:
            raise ValueError(f"Parameter value '{str(self.config.col_name)}' is not a valid collection name")
        if not self.engine.exists_collection_with_name(self.config.col_name):
            log.info(f'Creating collection "{self.config.col_name}"')
            col_id = self.config.col_name
            self.engine.create_collection_and_wait(self.config.col_name, col_id)

    def _send_batch(self):
        if self.config.ayfie_json_dump_file:
            with open(self.config.ayfie_json_dump_file, "wb") as f:
                try:
                    f.write(pretty_formated({'documents': self.batch}).encode('utf-8')) 
                except Exception as e:
                    log.warning(f'Writing copy of batch to be sent to disk failed with the message: {str(e)}')    
        col_id = self.engine.get_collection_id(self.config.col_name)
        log.info(f'About to feed batch of {len(self.batch)} documents')
        return self.engine.feed_collection_documents(col_id, self.batch)
        
    def _add_fuzzy_noise(self):
        for document in self.batch:
            spread_type = randint(0,4)
            if spread_type == 0:
                max = 0x1F
            elif spread_type == 1:
                max = 0x7F
            elif spread_type == 2:
                max = 0xFF
            elif spread_type == 3:
                max = 0xFFFF
            else:
                max = 0x10FFFF   
            content = list(document['fields']['content'])
            for _ in range(randint(1, self.config.max_fuzzy_chars_per_doc)):
                content[randint(0, len(content) - 1)] = chr(randint(0, max)) 
            document['fields']['content'] = "".join(content) 

    def process_batch(self, is_last_batch=False):
        if self.config.file_copy_destination: 
            Os.make_dir(self.config.file_copy_destination) 
            for from_file_path in self.data_source.pop_file_paths():
                if self.config.file_copy_destination:
                    copy(from_file_path, self.config.file_copy_destination)
                elif False:  # Salted version of the line above
                    _, filename = path_split(from_file_path)
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
            if self.config.max_fuzzy_chars_per_doc:
                self._add_fuzzy_noise()
            try:
                response = self._send_batch()
                if self.config.report_doc_error:
                    if 'error' in response and response['error']:
                        self._print(pretty_formated(response))
            except MemoryError:
                batch_failed = True
                err_message = "Out of memory"
            except HTTPError as e: 
                batch_failed = True
                err_message = str(e)
                http_code = err_message.split('"')[1].split()[0]
                batch_sequence_number, batch = self.engine.get_last_batch()
                error_code_dir = join(CRASHED_BATCHES_DIR, http_code)
                crash_dir = FileTools.get_random_path(error_code_dir, f"batch_{batch_sequence_number}")
                Os.make_dir(crash_dir)
                Os.write_to_file_as_non_root_user(join(crash_dir, "crashed_batch.json"), batch)
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
        self.data_source.prepare_data_source()
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
            total_file_size, unit = get_unit_adapted_byte_figure(total_file_size)  
            log_message = f"A total of {file_count} files and {total_file_size} {unit} were retrieved from disk" + log_message
            log.info(log_message)
            self._print(log_message)
        return elapsed_time
         
    def process(self):
        report_time = False
        more_params = {}
        if type(self.config.processing) is dict:
            if 'report_time' in self.config.processing and self.config.processing['report_time']:
                report_time = True
                del self.config.processing['report_time']
            if 'filters' in self.config.processing:
                more_params['filters'] = self.config.processing['filters']
                del self.config.processing['filters']
        if self.config.progress_bar:
            settings = {}
            if self.config.processing:
                settings = {"processing": self.config.processing}
            job_id, processing_time = JobsHandler(self.config).job_processesing("PROCESSING", settings, more_params)
        else:
            col_id = self.engine.get_collection_id(self.config.col_name)
            processing_time = self.engine.process_collection_and_wait(col_id, self.config.processing)
        if report_time:
            self._report_time("processing", str(processing_time))
        return processing_time
            
    def feed_documents_and_commit(self):
        feeding_time = self.feed_documents()
        if self.config.feeding_report_time:
            self._report_time("feeding", str(feeding_time))
        if self.config.feeding_disabled:
            log.info(f'No commit or processing as feeding is disabled')
            return
        if self.doc_count <= 0:
            log.info(f'No documents to feed')
            return self.doc_count
        col_id = self.engine.get_collection_id(self.config.col_name)
        log.info(f'Fed {self.doc_count} documents. Starting to commit')
        self.engine.commit_collection_and_wait(col_id)
        log.info(f'Done committing')
        return self.doc_count

    def feed_documents_commit_and_process(self):
        numb_of_fed_docs = self.feed_documents_and_commit()
        if numb_of_fed_docs == 0:
            log.info(f'No documents to process')
        else:
            processing_time = self.process()
            if self.config.feeding_report_time:
                self._report_time("processing", str(processing_time))
            log.info(f'Done processing')


class Classifier(EngineConnector):

    def __create_and_train_classifiers(self, classification):
        if classification['name'] in self.engine.get_classifier_names():
            self.engine.delete_classifier(classification['name'])
        col_id = self.engine.get_collection_id(self.config.col_name) 
        self.engine.create_classifier(classification['name'], col_id, classification['training_field'],
                                     classification['min_score'], classification['num_results'],
                                     classification['training_filters'],
                                     classification['use_entities'],
                                     classification['use_tokens'])
        self.engine.train_classifier(classification['name'])
        self.engine.wait_for_classifier_to_be_trained(classification['name'])

    def create_classifier_job_and_wait(self, classification):
        col_id = self.engine.get_collection_id(self.config.col_name)
        if "dummy" in classification:
            del classification["dummy"]
        settings = {
            "classification": {
                "classifierId": classification["name"],
                "minScore": classification["min_score"],
                "numResults": classification["num_results"],
            } 
        }
        more_params = {
            "filters" : classification['execution_filters'],
            "outputField" : classification['output_field']
        }
        if self.config.progress_bar:
            job_id, processing_time = JobsHandler(self.config).job_processesing("CLASSIFICATION", settings, more_params)
            return processing_time
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
        doc_ids = self.get_doc_ids(col_id, query, random_order=True)
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
                "use_entities"         : classification['use_entities'],
                "use_tokens"           : classification['use_tokens'],
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
                        classification['use_entities'],
                        classification['use_tokens'],
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
            all_known_categories.append(f'{output_field}:"{known_category}"')
            all_known_categories_ored = " OR ".join(all_known_categories)
        for known_category in known_categories:
            all_other_known_categories = all_known_categories.copy()
            all_other_known_categories.remove(f'{output_field}:"{known_category}"')
            all_other_known_categories_ored = " OR ".join(all_other_known_categories)
        queries = {}
        for known_category in known_categories:
            queries[known_category] = {}
            queries[known_category]["no_category"] = f'{training_field}:"{known_category}" AND NOT ({all_known_categories_ored}) AND {data_set_field}:verification'
            for detected_category in known_categories:
                queries[known_category][detected_category] = f'{output_field}:"{detected_category}" AND {training_field}:"{known_category}" AND {data_set_field}:verification'              
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
            if self.config.progress_bar:
                job_id, sampling_time = JobsHandler(self.config).job_processesing("SAMPLING", self.config.sampling)
                job_report = self.engine.get_job(job_id)
            else:
                col_id = self.engine.get_collection_id(self.config.col_name)
                job_id = self.engine.start_sampling_and_wait(col_id, self.config.processing)
                job_report = self.engine.get_job(job_id)
            if not "sample" in job_report:
                raise DataFormatError('No "sample" item in job output')
            if self.config.sampling_report_time:
                self._report_time("sampling", str(sampling_time))
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
                job_id, clustering_time = JobsHandler(self.config).job_processesing("CLUSTERING", config)
            else:
                clustering_time = self.engine.process_collection_and_wait(col_id, config)
            self._report_time("clustering", clustering_time)     
        else:
            log.info('No clustering job')  
            
       
class Querier(EngineConnector):

    def search(self, search, retries=MAX_CONNECTION_RETRIES, col_id=None):
        if not col_id:
            col_id = self.engine.get_collection_id(self.config.col_name)
        if 'suggest_only' in search and search['suggest_only']:
            return self._get_query_suggestion(col_id, search, retries)
        if 'suggest_offset' in search and search['suggest_offset'] != None:
            response = self._get_query_suggestion(col_id, search, retries)
            try:
                search["query"] = response["suggestions"][0]["list"][search['suggest_offset']]["term"]
            except IndexError:
                return {}
            search = {k:v for k,v in search.items() if not k.startswith("suggest")}
        return self.engine.meta_search(col_id, search, retries)

    def _get_query_suggestion(self, col_id, search, retries=MAX_CONNECTION_RETRIES):
        if not search['suggest_limit'] and type(search['suggest_offset']) is int:
            search['suggest_limit'] = search['suggest_offset'] + 1
        return self.engine.get_query_suggestion(col_id, search['query'], search['suggest_offset'], 
                                                        search['suggest_limit'], search['suggest_filter'], retries)
 
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

    def _get_jobs_status(self):
        if self.config.server == OFF_LINE:
            raise ValueError("Not possible to request job status for off-line server")
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
                 
    def _get_logs_status(self):
        if self.config.report_logs_source and self.config.report_retrieve_logs:
            raise ValueError("Reporting options 'logs_source' and 'retrieve_logs' cannot both be set")
        if self.config.report_logs_source:
            if self.config.report_logs_source.startswith('http'):
                Os.recreate_dir(DOWNLOAD_DIR)
                log_file_path = WebContent.download(self.config.report_logs_source, DOWNLOAD_DIR)
            else:
                log_file_path = self.config.report_logs_source
        elif self.config.report_retrieve_logs:
            with change_dir(self.config.report_retrieve_logs):
                log_file = "inspector-logs.txt"
                execute(f"docker-compose logs -t > {log_file}")
                log_file_path = join(self.config.report_retrieve_logs, log_file)
        else:
            return

        if not exists(log_file_path):
            raise ValueError(f"'{log_file_path}' does not exist")
        LogAnalyzer(self.config, log_file_path, TMP_LOG_UNPACK_DIR).analyze()

    def _get_memory_config(self):
        pass

    def do_reporting(self):
        if self.config.report_jobs:
            self._get_jobs_status()
        if self.config.report_logs_source or self.config.report_retrieve_logs:
            self._get_logs_status()
      

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
            document_update_time = self.__update_documents_by_query(docs_update)
            if docs_update["report_time"]:
                self._report_time("document update", str(document_update_time))
                log.info(message)
                self._print(message)


class LogAnalyzer():

    def __init__(self, config, log_file_path=None, log_unpacking_dir=None):
        self.config = config
        self.log_file = log_file_path
        self.log_unpacking_dir = log_unpacking_dir
        self.unzip_dir = UNZIP_DIR
        self.queries = {}
        if not exists(self.log_file):
            raise ConfigError(f"There is no {self.log_file}")
        if self.config.report_log_splitting:
            Os.make_dir(self.config.report_log_splitting_dir)
        self.filtered_patterns = [
            "^.*RestControllerRequestLoggingAspect: triggering JobController.getJob\(\.\.\) with args:.*$",
            "^.*Performing partial update on document.*$"
        ] 
        self.inspector_version = None
        self.system_variables = [
            {
                "pattern" : r"^.*main platform.config.AyfieVersion: Version: (.*) Buildinfo: [^\$]*\n$",            
                "variable": self.inspector_version 
            }
        ]
        self.system_info = [ 
            {
                "pattern" : r"^.*jobId=([0-9]+),jobState=SCHEDULED]$",
                "extraction": [("Scheduled jobs", 1)], 
                "key": "scheduled jobs",
                "accumulate": []
            },       
            {
                "pattern" : r"^.*jobId=([0-9]+),jobState=RUNNING]$",
                "extraction": [("Running jobs", 1)],               
                "key": "started jobs",
                "accumulate": []
            },
            {
                "pattern" : r"^.*jobId=([0-9]+),jobState=SUCCEEDED]$",
                "extraction": [("Successful jobs", 1)],               
                "key": "successful jobs",
                "accumulate": []
            },
            {
                "pattern" : r"^.*type=[A-Z]+,.*jobId=([0-9]+),jobState=FAILED\]$",
                "extraction": [("Failed jobs", 1)],               
                "key": "failed jobs",
                "accumulate": []
            },
            {
                "pattern" : r"^.*type=[A-Z]+_[_A-Z]+,.*jobId=([0-9]+),jobState=FAILED\]$",
                "extraction": [("Failed sub-jobs", 1)],               
                "key": "failed sub-jobs",
                "accumulate": []
            },
            {
                "pattern" : r"^.*ingestion\.service\.DefaultDocumentFeedingService: feeding in collection \".+\" a batch of ([0-9]+) documents$",
                "extraction": [("Documents across batches", 1)],
                "sum": [],
                "key": "fed batches"
            },
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
                "extraction": [("First disk space report", 1), ("First disk capacity report", 2)],
                "occurence": "first"
            }, 
            {
                "pattern" : r"^.*free: (.*gb)\[(.*%)\].*$",
                "extraction": [("Last disk space report", 1), ("Last disk capacity report", 2)],
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
                "pattern" : r"^.*main platform.config.AyfieVersion: (Version: .* Buildinfo: [^\$]*)\n$",            
                "extraction": [("ayfie Inspector", 1)] 
            },
            {
                "pattern" : r"^.*initializing general extractor with name _gdpr.*$",
                "extraction": [("GDPR", "Enabled")]
            },
            {
                "pattern" : r"^.*skipExtractors=\[_gdpr\].*$",
                "extraction": [("GDPR", "Disabled")]
            },
            {
                "pattern" : r"^.*SPARK_WORKER_MEM_LIMIT=([0-9]+G).*$",
                "extraction": [("SPARK_WORKER_MEM_LIMIT", 1)]
            },            
            {
                "pattern" : r"^.*SPARK_DRIVER_MEMORY=([0-9]+G).*$",
                "extraction": [("SPARK_DRIVER_MEMORY", 1)]
            },
            {
                "pattern" : r"^.*SPARK_EXECUTOR_MEMORY=([0-9]+G).*$",
                "extraction": [("SPARK_EXECUTOR_MEMORY", 1)]
            },
            {
                "pattern" : r"^.*SPARK_WORKER_MEMORY=([0-9]+G).*$",
                "extraction": [("SPARK_WORKER_MEMORY", 1)]
            },
            {
                "pattern" : r"^.*API_MEM_LIMIT=([0-9]+G).*$",
                "extraction": [("API_MEM_LIMIT", 1)]
            },
            {
                "pattern" : r"^.*ELASTICSEARCH_MEM_LIMIT=([0-9]+G).*$",
                "extraction": [("ELASTICSEARCH_MEM_LIMIT", 1)]
            },
            {
                "pattern" : r"^.*ELASTICSEARCH_MX_HEAP=([0-9]+G).*$",
                "extraction": [("ELASTICSEARCH_MX_HEAP", 1)]
            },
            {
                "pattern" : r"^.*RECOGNITION_MEM_LIMIT=([0-9]+G).*$",
                "extraction": [("RECOGNITION_MEM_LIMIT", 1)]
            },
            {
                "pattern" : r"^.*RECOGNITION_MX_HEAP=([0-9]+G).*$",
                "extraction": [("RECOGNITION_MX_HEAP", 1)]
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
                "indication": "that Elasticsearch and/or the host is low on memory. If that is the case, then increasing ELASTICSEARCH_MEM_LIMIT and ELASTICSEARCH_MX_HEAP and/or adding more RAM to the host, could possibly solve it."
            },
            {
                "pattern" : r"^.*java\.lang\.OutOfMemoryError.*$",
                "indication": "the JVM has run out of memory."
            },  
            {
                "pattern" : r"^.*java\.lang\.IllegalStateException: unread block data.*$",
                "indication": "the JVM has run out of memory."
            },           
            {
                "pattern" : r"^.*all nodes failed.*$",
                "indication": "more data is passed from Spark to Elasticsearch then what the disk is able to handle due to a combination of 4 things 1) the speed of the disk, 2) the amount of data passed per transfer operation, 3) the timeout limit set for that transfer operation, and 4) the number of CPUs (more CPU's, more data ElasticSearach will be receiving."
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
                "indication": "the system is low on disk and Elasticsearch has write protecting the index to prevent possible index corruption. Check out http://docs.ayfie.com/ayfie-platform/documentation/api.html#_cleanup_after_disk_full for how to recover."
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
                "indication": "the system is low on disk and Elasticsearch has write protecting the index to prevent possible index corruption. Check out http://docs.ayfie.com/ayfie-platform/documentation/api.html#_cleanup_after_disk_full for how to recover."
            },
            {
                "pattern" : r"^.*java.io.IOException: (No space left on device|Cannot allocate memory).*$",
                "indication": "that one is out of disk space."
            },
            {
                "pattern" : r"^.*flood stage disk watermark [[0-9]{1,20%] exceeded on.*$",
                "indication": "that one is out of disk space."
            },
            {
                "pattern" : r"^.*(jobState=FAILED|,description=Execution failed: ).*$",
                "indication": "a job has failed. Use the jobId(s) above to look up further failure details using 'curl <server>:<port>/ayfie/v1/jobs/<jobId>?verbose=true' after replacing <jobId> with the id of the failing sub-job (or the job in absence of any sub-job).",
                "queue_length": 2
            },
            {
                "pattern" : r"^.*hs_err_pid[0-9]+\.log.*$",
                "indication": "there is a JVM process crash report at the given location within the given container."
            },
            {
                "pattern" : r"^.*fatal error on the network layer.*$",
                "indication": "we came across an possible error indication that we have seen before, but that we yet not fully understand."
            },
            {
                "pattern" : r"^.*Total size of serialized results.+bigger than spark.driver.maxResultSize.*$",
                "indication": "one has run into a hard coded mem limit that has been removed in later versions."
            },
            {
                "pattern" : r"^.*requirement failed: The vocabulary size should be > 0.*$",
                "indication": "a classifier training job has failed due to there not being any tagged documents in the training set or that there were not sufficient entities and/or tokens in the tagged documents to do the training."
            },
            {
                "pattern" : r"^.*numHashTables given invalid value.*$",
                "indication": "bad or errounous job parameter values have been used for near dupe detection. For instance, if the parameter 'minimalSimilarity' were to be set to let's say '0.9' instead of '90.0', that could in some circumstance cause this message."
            },
            {
                "pattern" : r"^.*ayfie\.platform\.digestion\.jobs\.clustering\.joining\.LSHSimilarityAttributeJoiner\.join\(LSHSimilarityAttributeJoiner\.java:.*$",
                "indication": "the not cusotmer documented parameter 'hashtable' value needs to be reduced high."
            },   
            {
                "pattern" : r"^.*RecognizeDictionaryWriter: Failed to create recognition dictionary!$",
                "indication": "the dictionay write operation failed. This is a known issue for large collection that was fixed in 2.1.0 of Inspector."
            },
            {
                "pattern" : r"^.*I/O error on POST request for \"[^ ]+\": Read timed out;.*$",
                "indication": "the Spark read or write operation timed out."
            },
            {
                "pattern" : r"^.*java.lang.IllegalStateException: Subprocess exited with status 137. Command ran: /extraction/[a-z]{2,3}/ayfie-wrapper.sh$",
                "indication": 'the entity extraction ran out of memory, try reducing the amount of data / number of documents processed at the time: "settings":{"processing":{"entityExtraction":{"maxDocumentsPerPartition":1000}}'
            },
            {
                "pattern" : r"^.*org.elasticsearch.hadoop.rest.EsHadoopRemoteException: illegal_argument_exception: .*$",
                "indication": "one has run into an issue described in ticket PLATFORM-354 and that has been solved in Inspector version 2.2.0."
            },
            {
                "pattern" : r"^.*IllegalArgumentException: mapper \[ayfieProcessedVersion\] of different type, current_type \[text\], merged_type \[keyword\]$",
                "indication": "one has run into a known migration issue relating to the field 'ayfieProcessedVersion' that is described in internal ayfie ticket OEM-1647"
            },
            {
                "pattern" : r"^.*Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. Check driver logs for WARN messages.*$",
                "indication": "one has run into the issue described in ticket PLATFORM-414 given that the failing sub job is ET2_PROCESSING and the Inspector version is 2.4.1. In that case either move to a later version or do not feed in conversion indexes.",
                "before_version": "2.5.0",
                "after_version": "2.4.0"
            },
            {
                "pattern" : r"^.*restapp ERROR.*HttpClientErrorException: 400 Bad Request.*$",
                "indication": "one is feeding batches in parallel and one has run into a race condition (see PLATFORM-459).",
            },
            {
                "pattern" : r"^.*max virtual memory areas vm.max_map_count \[[0-9]+\] is too low, increase to at least \[[0-9]+\].*$",   
                "indication": "one has not set the variable vm.max_map_count as instructed in the Linux installation guide.",
            },
            {
                "pattern" : r"^.*Failed to establish a valid connection to elasticsearch.*$",   
                "indication": "the elasticsearch container is not responding.",
            },             
        ]
        for regex in self.config.report_custom_regexs:
            self.symptoms.append({
                "pattern" : regex,
                "indication": "custom"
            })  
        for variable in self.system_variables:
            variable["compiled_pattern"] = compile(variable["pattern"])            
        for symptom in self.symptoms:
            symptom["compiled_pattern"] = compile(symptom["pattern"])
        self.query_patterns = [
            {
                "title": QUERY_API_LOGS, 
                "pattern": r"^.*DefaultSearchQuery\[query=(.*),highlight.*$"
            },
            {
                "title": QUERY_SUGGEST_LOGS,
                "pattern": r"^.*suggest.service.ApproxindexSuggestService: External suggest call for \{query=\[(.*)\]\} returned with status 200.*$"
            },
            {
                "title": QUERY_SUGGEST_LOGS,
                "pattern": r"^.*SuggestController.suggestionsGet.+, \{.*query=\[(.*)\]\}, \(GET /ayfie/v1/collections/.+/suggestions.*$"
            },
        ]
        for query_pattern in self.query_patterns:
            query_pattern["compiled_pattern"] = compile(query_pattern["pattern"])
        self.compiled_date_pattern = compile("^.*\[0m (20[0-2][0-9]-[0-1][0-9]-[0-3][0-9])T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9]{9,9}Z .*$")
  
    def _clear_counters(self):
        for symptom in self.symptoms:
            symptom["occurences"] = 0
            symptom["last_occurence_line"] = None
            symptom["last_occurence_samples"] = deque([])
            symptom["before_version"] = None
            symptom["after_version"] = None
        self.errors_detected = False
        
    def _prepare_temp_log_directory(self, log_file, log_dir):
        file_type, detected_encoding = FileTools.get_file_type(log_file)
        if FileTools.unzip(log_file, self.unzip_dir, file_type): 
            Os.del_dir(log_dir)
            copy_dir_tree(self.unzip_dir, log_dir)
        else:
            Os.recreate_dir(log_dir)
            copy(log_file, log_dir)
            
    def _line_filtering(self, line):
        if len(line.strip()) == 0:
            return "".encode("utf-8")
        for pattern in self.filtered_patterns:
            m = match(pattern, line)
            if m:
                return "".encode("utf-8")
        return line.encode("utf-8")
    
    def _process_log_file(self, log_file):
        if not exists(log_file):
            raise ValueError(f"Path '{log_file}' does not exists")
        if isdir(log_file):
            raise ValueError(f"'{log_file}' is not a file, but a directory")
        file_type, detected_encoding = FileTools.get_file_type(log_file) 
        encodings = ["utf-8", "iso-8859-1"]
        if detected_encoding:
            if detected_encoding == "utf_16_le":
                detected_encoding = "utf-16"
            if not detected_encoding in encodings:
                encodings = [detected_encoding] + encodings
        log.debug(f"Ordered encoding list: {str(encodings)}")
        for encoding in encodings:
            log.debug(f"Now decoding file '{log_file}' using encoding '{encoding}'")
            try:
                self._process_encoded_log_file(log_file, encoding)
            except UnicodeDecodeError:
                log.debug("UnicodeDecodeError")
                continue
            except DataFormatError:
                log.debug("DataFormatError")
                raise
            return
         
    def _log_file_verification(self, log_file_name, line_count, line):
        if line_count == 0 and not self.config.report_skip_format_test:
            if not line.strip().startswith(LOG_FILE_START_STRING):
                raise DataFormatError(f"File '{log_file_name}' is not recognized as a ayfie Inspector log file")
                     
    def _query_extraction(self, line):
        for query_pattern in self.query_patterns:
            m = query_pattern["compiled_pattern"].match(line) 
            if m:
                query = m.group(1)
                if self.config.report_remove_query_part:
                    hit = False
                    for query_part in self.config.report_remove_query_part:
                        m = match(query_part, query)
                        if m:
                            query = query.replace(m.group(1), "")
                        else:
                            break
                query = query.strip().lower()
                if not query_pattern["title"] in self.queries:
                    self.queries[query_pattern["title"]] = {}
                item_dict = self.queries[query_pattern["title"]]
                date = None
                if self.config.report_queries_by_date:
                    m = match(self.date_pattern, line)
                    if m:
                        date = m.group(1)
                        if not date in self.queries[query_pattern["title"]]:
                            self.queries[query_pattern["title"]][date] = {}
                        item_dict = self.queries[query_pattern["title"]][date]
                    else:
                        raise AutoDetectionError(f"Query log line does not have a date: '{line}'")
                if not query in item_dict:
                    item_dict[query] = 0
                item_dict[query] += 1
                return True
        return False

    def _system_info_extraction(self, line): 
        for info in self.temp_system_info: 
            if "occurence" in info and info["occurence"] == "completed":
                continue
            m = info["compiled_pattern"].match(line)
            if m:
                output_line = []
                if "count" in info:
                    info["count"] += 1
                for item in info["extraction"]:
                    item_name = item[0]
                    item_value = item[1]
                    if type(item_name) is int:
                        item_name = m.group(item_name)
                    if item_value:
                        if type(item_value) is int:
                            item_value = m.group(item_value)
                    elif "count" in info:
                        item_value = info["count"]
                    else:
                        raise ValueError("Item_value set to None requires parameter count to be initialized")
                    if "accumulate" in info:
                        info["accumulate"].append(item_value)
                    elif "sum" in info:
                        info["sum"].append(int(item_value))
                        output_line.append(f"{item_name} ({len(info['sum'])}): {sum(info['sum'])}")
                    else:
                        output_line.append(f"{item_name}: {item_value}")
                if "key" in info:
                    if "accumulate" in info:
                        self.info_pieces[info["key"]] = info["accumulate"]
                    elif output_line:
                        self.info_pieces[info["key"]] = " ".join(output_line)
                else:
                    self.info_pieces[", ".join(output_line)] = False
                if "occurence" in info and info["occurence"] == "first":
                    info["occurence"] = "completed"
        
    def _failure_detection(self, line):
        line_contains_failure_indicator = False
        for symptom in self.symptoms:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                m = symptom["compiled_pattern"].match(line)
                if m:
                    line_contains_failure_indicator = False
                    self.errors_detected = True
                    symptom["occurences"] += 1
                    symptom["last_occurence_line"] = self.line_count
                    symptom["last_occurence_samples"].append(m.group(0))
                    queue_length = 1
                    if "queue_length" in symptom:
                        queue_length = symptom["queue_length"]
                    while len(symptom["last_occurence_samples"]) > queue_length:
                        symptom["last_occurence_samples"].popleft()  
                    if symptom["indication"] == 'custom':
                        if self.config.report_dump_custom_lines:
                            print(m.group(0))
                    elif self.config.report_dump_all_lines:
                        print(m.group(0))
        return line_contains_failure_indicator
                        
    def _log_file_filtering_and_splitting(self, line):
        if self.config.report_noise_filtering:
            self.filtered_file.write(self._line_filtering(line))
        if self.config.report_log_splitting:
            m = match(self.date_pattern, line)
            if m:
                date = m.group(1)
                if self.config.report_log_split_date_filter:
                    if not date in self.config.report_log_split_date_filter:
                        return
                filepath = join(self.config.report_log_splitting_dir, date + ".log")
                if not filepath in self.file_handle_lookup:
                    self.file_handle_lookup[filepath] = open(filepath, "ab")
                    start_line = LOG_FILE_START_STRING + "\n"
                    self.file_handle_lookup[filepath].write(start_line.encode("utf-8"))
                self.file_handle_lookup[filepath].write(line.encode("utf-8"))
                
    def set_system_variables(self, line):
        for variable in self.system_variables:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                m = variable["compiled_pattern"].match(line)
                if m:
                    variable["variable"] = m.group(1)
                  
    def _process_encoded_log_file(self, log_file, encoding):
        if self.config.report_query_statistic_per_file:
            self.queries = {}
        self.temp_system_info = deepcopy(self.system_info)
        for info in self.temp_system_info:
            info["compiled_pattern"] = compile(info["pattern"])
        self.file_handle_lookup = {}
        with open(log_file, "r", encoding=encoding) as f:
            if self.config.report_noise_filtering:
                output_file = log_file + ".filtered"
            else:
                Os.make_dir(DUMMY_DIR)
                output_file = join(DUMMY_DIR, "dummy.txt")
            Os.write_to_file_as_non_root_user(output_file, "".encode("utf-8"))
            with open(output_file, "w") as self.filtered_file:
                pass
            with open(output_file, "ab") as self.filtered_file:
                self.line_count = 0
                self.info_pieces = {}
                for line in f:
                    self._log_file_verification(log_file, self.line_count, line) 
                    self.line_count += 1
                    self._log_file_filtering_and_splitting(line)
                    if self.config.report_max_log_lines and self.line_count >= self.config.report_max_log_lines:
                        return
                    self.set_system_variables(line)
                    if self.config.report_queries:
                        if self._query_extraction(line):
                            continue
                    if self._failure_detection(line):
                        continue
                    self._system_info_extraction(line)
        for file in self.file_handle_lookup:
            self.file_handle_lookup[file].close()
        
    def _filter_out_substrings(self, data_dict):
        strings = [string.strip() for string in list(data_dict.keys())]
        strings.sort(key=lambda string: len(string), reverse=True)
        output = []
        for string in strings:
            if not any([s.startswith(string) for s in output]):
                output.append(string)
        new_dict = {}
        for string in data_dict:
            if string in output:
                new_dict[string] = data_dict[string]
        return new_dict
        
    def _produce_output(self, title, data_dict, format, delimiter=';'): 
        output = ""
        if len(data_dict) > 0:
            if title:
                output = f"\n\n====== {title} ======\n"
            if format == "single_value":
                for key in data_dict.keys():
                    if data_dict[key]:
                        output += f"{data_dict[key]}\n"
                    else:
                        output += f"{key}\n"
            elif format == CSV:  
                for key in data_dict.keys():
                    output += f"{key}{delimiter}{data_dict[key]}\n"           
            elif format == JSON:
                if self.config.report_queries_by_date:
                    for date in data_dict.keys():
                        data_dict[date] = self._filter_out_substrings(data_dict[date])
                else:
                    data_dict = self._filter_out_substrings(data_dict)
                output += pretty_formated(data_dict)
            else:
                raise ValueError("'{format}' is an unknown format")
            output += "\n"
        return output
        
    def _format_item_output_string(self, data_dict, key):
        if key in data_dict:
            if data_dict[key] == []:
                del data_dict[key]
            else:
                data_dict[key] = f"{key} ({len(data_dict[key])}): {', '.join(data_dict[key])}" 
            
    def _prepare_item_output(self, data_dict, key, starting_set, end_set):
        data_dict[key] = [x for x in starting_set if x not in end_set]
        self._format_item_output_string(data_dict, key)
        
    def _get_data_dict_entry(self, data_dict, key):
        if key in data_dict:
            return data_dict[key]
        return []
        
    def _post_processing_statistics(self, data_dict):
        failed_jobs = self._get_data_dict_entry(data_dict, "failed jobs")
        successful_jobs = self._get_data_dict_entry(data_dict, "successful jobs")
        failed_sub_jobs = self._get_data_dict_entry(data_dict, "failed sub-jobs")
        scheduled_jobs = self._get_data_dict_entry(data_dict, "scheduled jobs")
        started_jobs = self._get_data_dict_entry(data_dict, "started jobs")
        terminated_jobs = failed_jobs + successful_jobs + failed_sub_jobs
        self._prepare_item_output(data_dict, "non-run scheduled jobs", scheduled_jobs, started_jobs + terminated_jobs)
        self._prepare_item_output(data_dict, "non-finished jobs", started_jobs, terminated_jobs)
        self._format_item_output_string(data_dict, "failed jobs") 
        self._format_item_output_string(data_dict, "failed sub-jobs") 
        for key in ["scheduled jobs", "started jobs", "successful jobs"]:
            if self.config.report_show_all_job_ids:
                self._format_item_output_string(data_dict, key) 
            elif key in data_dict:
                del data_dict[key]
            
    def _dump_output(self, output, output_type=None): 
        if self.config.report_output_destination == TERMINAL_OUTPUT:
            print(output)
        elif self.config.report_output_destination == SILENCED_OUTPUT:
            return
        else:
            filename = self.config.report_output_destination
            if output_type == QUERY_API_LOGS:
                filename += ".query"
            elif output_type == QUERY_SUGGEST_LOGS: 
                filename += ".suggest"
            else:
                filename += ".txt"
            Os.write_to_file_as_non_root_user(filename, output.encode('utf-8'))                
         
    def analyze(self):
        if isfile(self.log_file):
            self._prepare_temp_log_directory(self.log_file, self.log_unpacking_dir)
        elif isdir(self.log_file):
            self.log_unpacking_dir = self.log_file
        else:
            ValueError(f"'{self.log_file}' is neither a file nor a directory")
        analyses_output = ""
        for log_file in FileTools.get_next_file(self.log_unpacking_dir):
            self._clear_counters()
            is_log_file = True
            is_empty_file = False
            number_of_line_info = ""
            try:
                self._process_log_file(log_file)
                self._post_processing_statistics(self.info_pieces)
                number_of_line_info = f'({self.line_count} LINES) '
                if self.line_count == 0:
                    is_log_file = False
                    is_empty_file = True
            except DataFormatError:
                is_log_file = False
            analyses_output += f'\n\n====== FILE "{log_file}" {number_of_line_info}======\n'
            if is_log_file:
                analyses_output += self._produce_output("Time, System & Operational Information", self.info_pieces, "single_value")
                if self.config.report_query_statistic_per_file:
                    for title in self.queries:
                        output = self._produce_output(None, self.queries[title], self.config.report_query_statistic_format)
                        self._dump_output(output, title)
                if self.errors_detected:
                    for symptom in self.symptoms:
                        if symptom["occurences"] > 0:
                            analyses_output += "\n====== Failure Indicator ======\n"
                            analyses_output += f'There are {symptom["occurences"]} occurrences (last one on line {symptom["last_occurence_line"]}) of this message:\n\n'
                            for occurence in symptom["last_occurence_samples"]:
                                analyses_output += f'{occurence}\n\n'
                            if symptom["indication"] == "custom":
                                 analyses_output += f'These are matches to custom regex "{symptom["pattern"]}"\n'
                            else:
                                analyses_output += f'The message could possibly indicate that {symptom["indication"]}\n'
                else:
                    analyses_output += "\n  NO KNOWN ERRORS DETECTED"
            elif is_empty_file:
                analyses_output += "\n  THE FILE DOES NOT SEEM TO HAVE ANY CONTENT"
            else:
                analyses_output += "\n  DOES NOT SEEM TO BE AN AYFIE INSPECTOR LOG FILE"
        
        if not self.config.report_query_statistic_per_file:
            for title in self.queries:
                output = self._produce_output(None, self.queries[title], self.config.report_query_statistic_format)
                self._dump_output(output, title)
        self._dump_output(analyses_output)
                      
    def get_diagnostic_page(self):
        page = "<html><head></head><body>"
        page = "<tr><table><th>SYMPTOM</th><th>DIAGNOSES</th></tr>"
        for symptom in self.symptoms:
            page += f"<tr><td>symptom.pattern</td><td>symptom.indication</td></tr>"
        page += "</table><br>" 
        return page
 
 
class Monitoring(EngineConnector):

    def _post_slack_message(self, message, web_hooks):
        for web_hook in web_hooks:
            try:
                requests.post(web_hook, data=dumps({"text":message}))
            except (gaierror, requests.exceptions.ConnectionError) as e:
                self._post_terminal_message(f"Error sending slack message: '{str(e)}'")
                
    def _post_email_message(self, message, recipients):
        raise NotImplementedError("Email alerting has still not been implemented")

    def _post_terminal_message(self, message):
        print(message)

    def _post_message(self, message, message_type, message_destination):
        if message_type == TERMINAL:
            self._post_terminal_message(message)
        elif message_type == EMAIL:
            self._post_email_message(message, message_destination)
        elif message_type == SLACK:
            self._post_slack_message(message, message_destination)
        else:
            raise ValueError(f"'{message_type}' is not a known message type") 
        
    def _system_is_up(self, monitoring):
        if int((sum(monitoring["monitor_fifo"]) / monitoring["queue_length"]) * 100) < monitoring["min_score"]:
            return False
        else:
            return True
        
    def _get_page(self, monitoring):
        return WebContent.download(monitoring["endpoint"])
        
    def _page_is_ok(self, result, monitoring):
        m = match(monitoring["regex"], result.decode(monitoring["encoding"]), DOTALL)
        if m:
            return True
        else:
            return False
            
    def _get_search_result(self, monitoring):
        config = {
            "engine":      self.config.engine,
            "server":      monitoring["server"],
            "port":        monitoring["port"],
            "api_version": self.config.api_version,
            "user":        self.config.user,
            "password":    self.config.password
        }
        querier = Querier(Config(config))
        return querier.search(monitoring["search"], retries=0, col_id=monitoring["col_id"]) 
        
    def _search_result_is_ok(self, result, monitoring):
        if int(result["meta"]["totalDocuments"]) >=  int(monitoring["min_docs"]):
            return True
        else:
            return False
            
    def _search_suggest_is_ok(self, result, monitoring):
        if int(result["count"]) >= int(monitoring["min_docs"]):
            return True
        else:
            return False
            
    def _get_alert_target(self, monitoring):
        if monitoring["endpoint"]:
            return monitoring["endpoint"]
        else:
            target_type = "query" if monitoring["query"] else "suggest"
            return f'{monitoring["col_id"]} @ {monitoring["server"]}:{monitoring["port"]} ({target_type})' 
      
    def _alert_if_failure(self, monitoring, result_obtaining_func, result_verification_func):
        result = None
        try:
            result = result_obtaining_func(monitoring)
        except (HTTPError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, 
                           requests.exceptions.ChunkedEncodingError, gaierror) as e:
            monitoring["monitor_fifo"].append(0)
        if result:  
            if result_verification_func(result, monitoring):
                monitoring["monitor_fifo"].append(1)
            else:
                monitoring["monitor_fifo"].append(0)
        monitoring["monitor_fifo"].popleft()
                
        if 0 in monitoring["monitor_fifo"]:
            print(f"{str(datetime.now())[:-7]} - {self._get_alert_target(monitoring)}")
            print(monitoring["monitor_fifo"])
            print
        
        action = "obtain suggestions for"
        if monitoring["query"]:
            action = "query"
        collection = monitoring["col_id"]
        server = monitoring["server"]
        port = monitoring["port"]
        if self._system_is_up(monitoring):
            if not monitoring["system_up"]:
                monitoring["system_up"] = True
                message = f'WORKING AGAIN: Now able to {action} {self.config.engine} collection "{collection}" at "{server}:{port}"'
                if monitoring["endpoint"]:
                    message = f'WORKING AGAIN: Now able to dowlload {monitoring["endpoint"]} successfully'
                for message_type in monitoring[ALERTS]:
                    self._post_message(message, message_type, monitoring[ALERTS][message_type]) 
        else:
            if monitoring["system_up"]:
                monitoring["system_up"] = False
                message = f'FAILURE: Unable to {action} {self.config.engine} collection "{collection}" at "{server}:{port}"'
                if monitoring["endpoint"]:
                    message = f'FAILURE: Unable to download {monitoring["endpoint"]} successfully'
                for message_type in monitoring[ALERTS]:
                    self._post_message(message, message_type, monitoring[ALERTS][message_type])

    def _alert_if_failing_query(self, search, monitoring):
        monitoring["search"] = search
        self._alert_if_failure(monitoring, self._get_search_result, self._search_result_is_ok)  

    def _alert_if_failing_suggest(self, search, monitoring):
        monitoring["search"] = search
        self._alert_if_failure(monitoring, self._get_search_result, self._search_suggest_is_ok)               
                
    def _alert_if_failing_page(self, monitoring):
        self._alert_if_failure(monitoring, self._get_page, self._page_is_ok)
        
    def monitor(self):
        print("Entering infinite monitoring loop")
        duplicates = []
        for monitoring in self.config.monitorings:
            monitor_fifo = []
            for i in range(monitoring["queue_length"]):   
                monitor_fifo.append(1)
            monitoring["monitor_fifo"] = deque(monitor_fifo) 
            monitoring["system_up"] = True
            if not monitoring["col_id"]:
                monitoring["col_id"] = self.config.col_name
            if not monitoring["server"]:
                monitoring["server"] = self.config.server
            if not monitoring["port"]:
                monitoring["port"] = self.config.port
            if not monitoring["user"]:
                monitoring["user"] = self.config.user
            if not monitoring["password"]:
                monitoring["password"] = self.config.password               
            if monitoring["query"] and monitoring["suggest"]:
                duplicate = deepcopy(monitoring)
                duplicate["suggest"] = False
                duplicates.append(duplicate)
                monitoring["query"] = False
            for notification_type in NOTIFICATION_TYPES:
                if monitoring[notification_type]:
                    for message_type in monitoring[notification_type]:
                        if notification_type == ALERTS:
                            msg = f'Alerting activated for {self._get_alert_target(monitoring)}'
                        elif notification_type == HEARTBEATS:
                            monitoring["last_heartbeat"] = time()
                            msg = f'A heartbeat every {monitoring["heartbeat_interval"]} seconds has been activated'
                        self._post_message(msg, message_type, monitoring[notification_type][message_type])
        self.config.monitorings += duplicates
        try:
            while True:
                for monitoring in self.config.monitorings:
                    if monitoring["query"]:
                        self._alert_if_failing_query({
                            "query" : "",
                            "size": 0
                        }, monitoring)
                    if monitoring["suggest"]:
                        self._alert_if_failing_suggest({
                            "query" : "jo",
                            "size": 0,
                            "suggest_only": True,
                            "suggest_limit": 1,
                            "suggest_offset": 0,
                            "suggest_limit": None,
                            "suggest_filter": None
                       }, monitoring)
                    if monitoring["endpoint"]:
                        self._alert_if_failing_page(monitoring)
                    if "last_heartbeat" in monitoring:
                        elapsed_time = int(time() - monitoring["last_heartbeat"])
                        if elapsed_time > monitoring["heartbeat_interval"]:
                            for message_type in monitoring[HEARTBEATS]:
                                self._post_message("Monitoring is still up and running", message_type, 
                                                                   monitoring[HEARTBEATS][message_type])
                            monitoring["last_heartbeat"] += monitoring["heartbeat_interval"]
                sleep(MONITOR_INTERVAL)
                
        except Exception as e:
            log.error(f"Monitoring is down due to an exepection: '{str(e)}'")
            for monitoring in self.config.monitorings:
                for message_type in monitoring[HEARTBEATS]:
                    self._post_message("Monitoring is down", message_type, monitoring[HEARTBEATS][message_type])
            raise
        log.error("Monitoring is down due to unknown reason")
            

class JobsHandler(EngineConnector):

    def _pre_init(self):
        if self.config.job_server:
            self.config.server = self.config.job_server
        if self.config.job_port:
            self.config.server = self.config.job_port
    
    def _start_job(self, job_type, settings, more_params={}):
        col_id = self.engine.get_collection_id(self.config.col_name)
        if job_type == "SAMPLING":  
            if not "filters" in settings:
                # Workaround required for the versions leading up to ayfie Inspector 2.11.0
                settings["filters"] = []
            job_id = self.engine.start_sampling(col_id, settings)
        else:
            config = {
                "collectionId" : col_id,
                "type" : job_type
            }
            if settings:
                config["settings"] = settings
            for key, value in more_params.items():
                config[key] = value
            job_id = self.engine.create_job(config)
        return job_id
        
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
        job_id = self._start_job(job_type, settings, more_params)
        return job_id, self.track_job(job_id, job_type)
        
    def __job_reporting_output_line(self, job, prefix, end="\n", tick=None):
        self._print(f' {prefix.rjust(8)}: {job["state"].ljust(10)} {job["id"].ljust(10)} {job["type"].ljust(22)}', end)

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
            pretty_print(self.engine.get_job(job_info["id"]))
        else:
            print(f'   -job: {job_info["state"]} {job_info["id"]} {job_info["type"]} {job_info["time_stamp"].replace("T", " ").split(".")[0]}')
        for sub_job_id in job_info['sub_jobs']:
            if verbose:
                print(pretty_formated(self.engine.get_job(sub_job_id)))
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

    NON_ACTION_CONFIG = ["server", 'port', 'col_name', 'config_source', 'silent_mode', "flags", "cmdline"]
    OFFLINE_ACTIONS   = ["installer", "uninstaller", "operational", "reporting"]

    def __init__(self, config):
        self.config_keys = list(config.keys())
        self.flags            = self._get_item(config, 'flags', [])
        self.engine           = self._get_item(config, 'engine', INSPECTOR)
        self.user             = self._get_item(config, 'user', None)
        self.password         = self._get_item(config, 'password', None) 
        self.silent_mode      = self._get_item(config, 'silent_mode', True)
        self.config_source    = self._get_item(config, 'config_source', None)
        self.server           = self._get_item(config, 'server', '127.0.0.1')
        self.job_server       = self._get_item(config, 'job_server', None)
        self.port             = self._get_item(config, 'port', '80')
        self.job_port         = self._get_item(config, 'job_port', None)
        self.api_version      = self._get_item(config, 'api_version', 'v1')
        self.col_name         = self._get_item(config, 'col_name', None)
        self.client_secret    = self._get_item(config, 'client_secret', None)
        self.csv_mappings     = self._get_item(config, 'csv_mappings', None)
        self.progress_bar     = self._get_item(config, 'progress_bar', True)
        self.infinite_loop    = self._get_item(config, 'infinite_loop', False) 
        self.document_update  = self._get_item(config, 'document_update', False)
        self.doc_retrieval    = self._get_item(config, 'document_retrieval', False)
        self._init_doc_retrieval(self.doc_retrieval)
        self.installer        = self._get_item(config, 'installer', None) 
        self._init_installer(self.installer)
        self.uninstaller      = self._get_item(config, 'uninstaller', None) 
        self._init_uninstaller(self.uninstaller)
        self.operational      = self._get_item(config, 'operational', None)
        self._init_operational(self.operational)
        self.processing       = self._get_item(config, 'processing', False)
        self._init_processing(self.processing)
        self.sampling         = self._get_item(config, 'sampling', False)        
        self._init_sampling(self.sampling)
        self.schema           = self._get_item(config, 'schema', None)
        self._init_schema(self.schema)
        self.feeding          = self._get_item(config, 'feeding', False)
        self._init_feeder(self.feeding)
        self.clustering       = self._get_item(config, 'clustering', False)
        self._init_clustering(self.clustering)
        self.classifications  = self._get_item(config, 'classification', False)
        self._init_classification(self.classifications)
        self.documents_updates= self._get_item(config, 'documents_update', False)
        self._init_documents_updates(self.documents_updates)
        self.searches         = self._get_item(config, 'search', False)
        self._init_search(self.searches)
        self.reporting        = self._get_item(config, 'reporting', False)
        self._init_report(self.reporting)
        self.monitoring       = self._get_item(config, 'monitoring', False)
        self._init_monitoring(self.monitoring)
        self.regression_testing = self._get_item(config, 'regression_testing', False)
        self._init_regression_testing(self.regression_testing)
        self._inputDataValidation()

    def __str__(self):
        varDict = vars(self)
        if varDict:
            return pretty_formated(vars(self))
        else:
            return {}

    def _get_item(self, config, item, default):
        if hasattr(self, item) and getattr(self, item) != None:
            return getattr(self, item)
        if config:
            if item in config and (config[item] or type(config[item]) in [bool, int, float]):
                if default in [None, False, [], {}] and config[item] == "null":
                    return default
                if type(default) is int:
                    return int(config[item])
                return config[item]
        return default

    def get_operation_specific_col_name(self, operation, col_name):
        op_specific_col_name = self._get_item(operation, 'col_name', None)
        if not op_specific_col_name:
            op_specific_col_name = col_name
        if not op_specific_col_name:
            raise ValueError("The feeding collection has to be set at some level in the config file")
        return op_specific_col_name
        
    def _init_installer(self, installer):
        self.docker_data_dir          = self._get_item(installer, 'docker_data_dir', None)
        self.gen_dot_env_file_only    = self._get_item(installer, 'gen_dot_env_file_only', False)
        self.start                    = self._get_item(installer, 'start', False)
        self.install                  = self._get_item(installer, 'install', False)
        self.installer_zip_file       = self._get_item(installer, 'installer_zip_file', None)
        self.processor_node           = self._get_item(installer, 'processor_node', False)
        self.primary_node             = self._get_item(installer, 'primary_node', False)
        self.primary_node_IP_or_FQDN  = self._get_item(installer, 'primary_node_IP_or_FQDN', None)
        self.simulation               = self._get_item(installer, 'simulation', False)
        self.version                  = self._get_item(installer, 'version', None)       
        self.os                       = self._get_item(installer, 'os', "auto")      
        self.docker_compose_yml_files = self._get_item(installer, 'docker_compose_yml_files', [DOCKER_COMPOSE_FILE])   
        self.enable_frontend          = self._get_item(installer, 'enable_frontend', True)
        self.external_network         = self._get_item(installer, 'external_network', False)
        self.enable_grafana           = self._get_item(installer, 'enable_grafana', False)
        self.enable_feeder            = self._get_item(installer, 'enable_feeder', False)
        self.disable_defender         = self._get_item(installer, 'disable_defender', True) 
        self.enable_gdpr              = self._get_item(installer, 'enable_gdpr', True) 
        self.enable_admin_page        = self._get_item(installer, 'enable_admin_page', False)
        self.enable_alt_threading     = self._get_item(installer, 'enable_alt_threading', False) 
        self.start_post_install       = self._get_item(installer, 'start_post_install', False)        
        self.security                 = self._get_item(installer, 'security', {})    
        self.ram_on_host              = self._get_item(installer, 'ram_on_host', 64)
        self.no_suggest_index         = self._get_item(installer, 'no_suggest_index', False)
        self.install_dir              = self._get_item(installer, 'install_dir', None)
        self.defender_exclude_paths   = listify(self._get_item(installer, 'defender_exclude_paths', None))
        self.docker_registry          = listify(self._get_item(installer, 'docker_registry', None))
        
    def _init_uninstaller(self, uninstaller):    
        self.stop                     = self._get_item(uninstaller, 'stop', False)
        self.uninstall                = self._get_item(uninstaller, 'uninstall', False)
        self.prune_system             = self._get_item(uninstaller, 'prune_system', False)
        self.nuke_everything          = self._get_item(uninstaller, 'nuke_everything', False)      
        self.install_dir              = self._get_item(uninstaller, 'install_dir', None) 
        self.version                  = self._get_item(uninstaller, 'version', None)
        
    def _init_operational(self, operational):
        self.file_transfer            = listify(self._get_item(operational, 'file_transfer', [])) 
        
    def _init_schema(self, schema_changes):
        if not schema_changes:
            self.schema_changes = None
            return 
        schema_changes = listify(schema_changes)          
        self.schema_changes = []
        for schema_change in schema_changes:
            self.schema_changes.append({
                "name"       : self._get_item(schema_change, 'name', None),
                "type"       : self._get_item(schema_change, 'type', "TEXT"),
                "list"       : self._get_item(schema_change, 'list', False),
                "roles"      : self._get_item(schema_change, 'roles', []),
                "properties" : self._get_item(schema_change, 'properties', []),
            })
        if len([1 for change in self.schema_changes if not "name" in change]):
            raise ConfigError('Schema configuration requires a field name to be set')
            
    def _init_feeder(self, feeding):
        self.data_source              = self._get_item(feeding, 'data_source', None)
        self.data_source              = FileTools.get_absolute_path(self.data_source, self.config_source)
        self.csv_mappings             = self._get_item(feeding, 'csv_mappings', self.csv_mappings)
        self.csv_list_value_delimiter = self._get_item(feeding, 'csv_list_value_delimiter', ";")
        self.include_headers_in_data  = self._get_item(feeding, 'include_headers_in_data', False)
        self.data_type                = self._get_item(feeding, 'data_type', AUTO)
        self.fallback_data_type       = self._get_item(feeding, 'fallback_data_type', None)
        self.prefeeding_action        = self._get_item(feeding, 'prefeeding_action', NO_ACTION)
        self.format                   = self._get_item(feeding, 'format', {})
        self.encoding                 = self._get_item(feeding, 'encoding', AUTO)
        self.batch_size               = self._get_item(feeding, 'batch_size', BATCH_SIZE)
        self.document_type            = self._get_item(feeding, 'document_type', None)
        self.preprocess               = self._get_item(feeding, 'preprocess', None)
        self.ayfie_json_dump_file     = self._get_item(feeding, 'ayfie_json_dump_file', None)
        self.report_doc_ids           = self._get_item(feeding, 'report_doc_ids', False)
        self.second_content_field     = self._get_item(feeding, 'second_content_field', None)
        self.file_picking_list        = self._get_item(feeding, 'file_picking_list', None)
        self.id_picking_list          = self._get_item(feeding, 'id_picking_list', [])
        self.file_copy_destination    = self._get_item(feeding, 'file_copy_destination', None)
        self.feeding_disabled         = self._get_item(feeding, 'feeding_disabled', False)
        self.report_fed_doc_size      = self._get_item(feeding, 'report_fed_doc_size', False)       
        self.max_doc_characters       = self._get_item(feeding, 'max_doc_characters', 0)
        self.min_doc_characters       = self._get_item(feeding, 'min_doc_characters', 0)       
        self.max_doc_size             = self._get_item(feeding, 'max_doc_size', 0)
        self.min_doc_size             = self._get_item(feeding, 'min_doc_size', 0)
        self.file_type_filter         = self._get_item(feeding, 'file_type_filter', None)
        self.file_extension_filter    = self._get_item(feeding, 'file_extension_filter', None)
        self.feeding_report_time      = self._get_item(feeding, 'report_time', False)
        self.report_doc_error         = self._get_item(feeding, 'report_doc_error', False)
        self.no_processing            = self._get_item(feeding, 'no_processing', False)
        self.email_address_separator  = self._get_item(feeding, 'email_address_separator', None)
        self.ignore_pdfs              = self._get_item(feeding, 'ignore_pdfs', True)
        self.convert_html_files       = self._get_item(feeding, 'convert_html_files', False)
        self.ignore_csv               = self._get_item(feeding, 'ignore_csv', False)
        self.treat_csv_as_text        = self._get_item(feeding, 'treat_csv_as_text', False)
        self.max_docs_to_feed         = self._get_item(feeding, 'max_docs_to_feed', None)
        self.doc_fragmentation        = self._get_item(feeding, 'doc_fragmentation', False)
        self.doc_fragment_length      = self._get_item(feeding, 'doc_fragment_length', 100)   
        self.max_doc_fragments        = self._get_item(feeding, 'max_doc_fragments', 10000) 
        self.fragmented_doc_dir       = self._get_item(feeding, 'fragmented_doc_dir', None) 
        self.convert_multipart_msg    = self._get_item(feeding, 'convert_multipart_msg', False) 
        self.flag_multipart_msg       = self._get_item(feeding, 'flag_multipart_msg', False)
        self.max_fuzzy_chars_per_doc  = self._get_item(feeding, 'max_fuzzy_chars_per_doc', 0)
        id_conv_table_file            = self._get_item(feeding, 'id_conversion_table_file', None)
        id_conv_table_delimiter       = self._get_item(feeding, 'id_conversion_table_delimiter', ",")
        id_conv_table_id_column       = self._get_item(feeding, 'id_conv_table_id_column', None)
        id_conv_table_filename_column = self._get_item(feeding, 'id_conv_table_filename_column', None)         
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
        
    def _init_processing(self, processing): 
        self.thread_min_chunks_overlap= self._get_item(processing, 'thread_min_chunks_overlap', None)
        self.near_duplicate_evaluation= self._get_item(processing, 'near_duplicate_evaluation', None)
        self.processing_report_time   = self._get_item(processing, 'report_time', False)
        
    def _init_sampling(self, sampling):
        if sampling:
            self.sampling = {
                "method"                    : self._get_item(sampling, 'method', "SMART"),
                "size"                      : self._get_item(sampling, 'size', 100),
                "smart_specificity"         : self._get_item(sampling, 'smartSpecificity', 0.3),
                "smart_dimensions"          : self._get_item(sampling, 'smartDimensions', 100),
                "smart_min_df"              : self._get_item(sampling, 'smartMinDf', 0.0005),
                "smart_presampling"         : self._get_item(sampling, 'smartPresampling', 100),
                "filters"                   : self._get_item(sampling, 'filters', []), 
            }
            self.sampling_report_time       = self._get_item(sampling, 'report_time', False)

    def _init_clustering(self, clustering):
        if not clustering:
            return
        c = {}
        c['dummy']                    = True
        c['clustersPerLevel']         = self._get_item(clustering, 'clustersPerLevel', None)
        c['documentsPerCluster']      = self._get_item(clustering, 'documentsPerCluster', None)
        c['maxRecursionDepth']        = self._get_item(clustering, 'maxRecursionDepth', None)
        c['minDf']                    = self._get_item(clustering, 'minDf', None)
        c['maxDfFraction']            = self._get_item(clustering, 'maxDfFraction', None)
        c = {key: c[key] for key in c if c[key] != None}
        self.clustering               = c
        self.clustering_filters       = self._get_item(clustering, 'filters', [])
        self.clustering_output_field  = self._get_item(clustering, 'outputField', '_cluster')
        self.clustering_report_time   = self._get_item(clustering, 'report_time', False)
        
    def _init_classification(self, classifications):
        if not classifications:
            return 
        self.classifications = []
        classifications = listify(classifications)
        for classification in classifications:
            self.classifications.append({
                "name"              : self._get_item(classification, 'name', None),
                "training_field"    : self._get_item(classification, 'trainingClassesField', 'trainingClass'),
                "min_score"         : self._get_item(classification, 'minScore', 0.6),
                "num_results"       : self._get_item(classification, 'numResults', 1),
                "use_entities"      : self._get_item(classification, 'useEntities', True),
                "use_tokens"        : self._get_item(classification, 'useTokens', False),
                "training_filters"  : self._get_item(classification, 'training_filters', []),
                "execution_filters" : self._get_item(classification, 'execution_filters', []),
                "output_field"      : self._get_item(classification, 'outputField', 'pathclassification'),
                "k-fold"            : self._get_item(classification, 'k-fold', None),
                "test_output_file"  : self._get_item(classification, 'test_output_file', join(AYFIE_EXPRESS_CFG_DIR, "test_output_file.html"))
            })

    def _init_documents_updates(self, documents_updates):
        self.docs_updates = []
        documents_updates = listify(documents_updates)
        for documents_update in documents_updates:
            self.docs_updates.append({
                "query"                  : self._get_item(documents_update, 'query', "*"),
                "action"                 : self._get_item(documents_update, 'action', ADD_VALUES),
                "field"                  : self._get_item(documents_update, 'field', 'trainingClass'),
                "values"                 : self._get_item(documents_update, 'values', []),
                "filters"                : self._get_item(documents_update, 'filters', []),
                "id_extraction_query"    : self._get_item(documents_update, 'id_extraction_query', "*"),
                "numb_of_docs_to_update" : self._get_item(documents_update, 'numb_of_docs', "ALL"),
                "report_time"            : self._get_item(documents_update, 'report_time', False)
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
                            
    def _init_doc_retrieval(self, doc_retrieval):
        self.doc_retrieval_always_use_search      = self._get_item(doc_retrieval, 'always_use_search', True)
        self.doc_retrieval_skip_empty_documents   = self._get_item(doc_retrieval, 'skip_empty_documents', False)
        self.doc_retrieval_output_file            = self._get_item(doc_retrieval, 'output_file', "output.csv")
        self.doc_retrieval_fields                 = self._get_item(doc_retrieval, 'fields', [])
        self.doc_retrieval_skip_if_field_empty    = self._get_item(doc_retrieval, 'skip_if_field_empty', [])
        self.doc_retrieval_skip_if_field_content  = self._get_item(doc_retrieval, 'skip_if_field_content', [])
        self.doc_retrieval_batch_size             = self._get_item(doc_retrieval, 'batch_size', "auto")
        self.doc_retrieval_csv_dialect            = self._get_item(doc_retrieval, 'csv_dialect', None)
        self.doc_retrieval_separator              = self._get_item(doc_retrieval, 'csv_separator', "|")
        self.doc_retrieval_query                  = self._get_item(doc_retrieval, 'query', "")
        self.doc_retrieval_filters                = self._get_item(doc_retrieval, 'filters', [])
        self.doc_retrieval_exclude                = self._get_item(doc_retrieval, 'exclude', [])
        self.doc_retrieval_max_docs               = self._get_item(doc_retrieval, 'max_docs', None)
        self.doc_retrieval_csv_header_row         = self._get_item(doc_retrieval, 'csv_header_row', True) 
        self.doc_retrieval_report_frequency       = self._get_item(doc_retrieval, 'report_frequency', 1000)  
        self.doc_retrieval_drop_underscore_fields = self._get_item(doc_retrieval, 'drop_underscore_fields', True) 
        self.doc_retrieval_field_merging          = self._get_item(doc_retrieval, 'field_merging', {}) 
        
        common_fields = list(set(self.doc_retrieval_skip_if_field_empty) & set(self.doc_retrieval_skip_if_field_content))
        if common_fields:
            common_fields = "', '".join(common_fields)
            raise ValueError(f"'skip_if_field_empty' and 'skip_if_field_content' cannot contain the same field name(s): '{common_fields}'")
        if self.doc_retrieval_csv_dialect:
            if not self.doc_retrieval_csv_dialect in csv.list_dialects():
                raise ValueError(f"'{self.doc_retrieval_}' is not csv dialect")
        else:
            self.doc_retrieval_csv_dialect = 'custom_config'
            csv.register_dialect(
                self.doc_retrieval_csv_dialect, 
                delimiter        = self._get_item(doc_retrieval, 'csv_delimiter', ','),
                doublequote      = self._get_item(doc_retrieval, 'csv_double_quote', True), 
                quotechar        = self._get_item(doc_retrieval, 'csv_quote_char', '"'), 
                lineterminator   = self._get_item(doc_retrieval, 'csv_line_terminator', '\r\n'), 
                escapechar       = self._get_item(doc_retrieval, 'csv_escape_char', None),
                quoting          = eval("csv." + self._get_item(doc_retrieval, 'csv_quoting', "QUOTE_MINIMAL")),
                skipinitialspace = self._get_item(doc_retrieval, 'csv_skip_initial_space', False)
            )

    def _init_search(self, searches):
        if not searches:
            self.searches = []
            return  
        self.searches = []
        searches = listify(searches)
        for search in searches:
            self.searches.append({
                "query_file"         : self._get_item(search, 'query_file', None),
                "result"             : self._get_item(search, 'result', "normal"),
                "result_destination" : self._get_item(search, 'result_destination', "display"),
                "classifier_field"   : self._get_item(search, 'classifier_field', None),
                "highlight"          : self._get_item(search, 'highlight', True),
                "sort_criterion"     : self._get_item(search, 'sort_criterion', "_score"),
                "sort_order"         : self._get_item(search, 'sort_order', "desc"),
                "minScore"           : self._get_item(search, 'minScore', 0.0),
                "exclude"            : self._get_item(search, 'exclude', []),
                "aggregations"       : self._get_item(search, 'aggregations', []),
                "filters"            : self._get_item(search, 'filters', []),
                "query"              : self._get_item(search, 'query', ""),
                "scroll"             : self._get_item(search, 'scroll', False),
                "size"               : self._get_item(search, 'size', 20),
                "offset"             : self._get_item(search, 'offset', 0),
                "suggest_only"       : self._get_item(search, 'suggest_only', False), 
                "suggest_offset"     : self._get_item(search, 'suggest_offset', None), 
                "suggest_limit"      : self._get_item(search, 'suggest_limit', None), 
                "suggest_filter"     : self._get_item(search, 'suggest_filter', None)                
            })

    def _init_report(self, report):
        self.report_logs_source       = self._get_item(report, 'logs_source', False)
        self.report_skip_format_test  = self._get_item(report, 'skip_format_test', False)
        self.report_logs_source       = FileTools.get_absolute_path(self.report_logs_source, self.config_source)
        self.report_output_destination= self._get_item(report, 'output_destination', TERMINAL_OUTPUT)
        self.report_verbose           = self._get_item(report, 'verbose', False)
        self.report_jobs              = self._get_item(report, 'jobs', False)
        self.report_noise_filtering   = self._get_item(report, 'noise_filtering', False)
        self.report_retrieve_logs     = self._get_item(report, 'retrieve_logs', False)
        self.report_dump_all_lines    = self._get_item(report, 'dump_all_lines', False)
        self.report_dump_custom_lines = self._get_item(report, 'dump_custom_lines', False)
        self.report_jobs_overview     = self._get_item(report, 'jobs_overview', False)
        self.report_show_all_job_ids  = self._get_item(report, 'show_all_job_ids', False)
        self.report_custom_regexs     = listify(self._get_item(report, 'custom_regexs', []))
        self.report_max_log_lines     = self._get_item(report, 'max_log_lines', None)
        self.report_resource_usage    = self._get_item(report, 'resource_usage', 0)  
        self.report_queries           = self._get_item(report, 'report_queries', False)
        self.report_queries_by_date   = self._get_item(report, 'queries_by_date', False)
        self.report_remove_query_part = listify(self._get_item(report, 'remove_query_part', None))          
        self.report_log_splitting     = self._get_item(report, 'log_splitting', False)
        self.report_log_splitting_dir = listify(self._get_item(report, 'log_splitting_dir', "split_log_files"))
        self.report_log_split_date_filter = self._get_item(report, 'log_split_date_filter', None) 
        self.report_query_statistic_format = self._get_item(report, 'query_statistic_format', CSV) 
        self.report_query_statistic_per_file = self._get_item(report, 'query_statistic_per_file', True)          
        if self.report_log_split_date_filter:
            yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            self.report_log_split_date_filter = [yesterday if date == YESTERDAY else date for date in self.report_log_split_date_filter]
            
    def _init_monitoring(self, monitorings):
        if not monitorings:
            self.monitorings = []
            return  
        self.monitorings = [] 
        for monitoring in listify(monitorings):
            self.monitorings.append({  
                "endpoint"           : self._get_item(monitoring, 'endpoint', None),
                "encoding"           : self._get_item(monitoring, 'encoding', "utf-8"),
                "regex"              : self._get_item(monitoring, 'regex', "^.*$"),
                "query"              : self._get_item(monitoring, 'query', None),
                "suggest"            : self._get_item(monitoring, 'suggest', None),
                "col_id"             : self._get_item(monitoring, 'col_id', None),
                "server"             : self._get_item(monitoring, 'server', None),
                "port"               : self._get_item(monitoring, 'port', None),  
                "user"               : self._get_item(monitoring, 'user', None),
                "password"           : self._get_item(monitoring, 'password', None),                 
                "min_docs"           : self._get_item(monitoring, 'min_docs', 0),
                "queue_length"       : self._get_item(monitoring, 'queue_length', 10),
                "alerts"             : self._get_item(monitoring, 'alerts', {}),
                "min_score"          : self._get_item(monitoring, 'min_score', 50),
                "heartbeats"         : self._get_item(monitoring, 'heartbeats', {}),
                "heartbeat_interval" : self._get_item(monitoring, 'heartbeat_interval', 86400)
            })
            for monitoring in self.monitorings:
                for notification_type in NOTIFICATION_TYPES:
                    if monitoring[notification_type]:
                        if not type(monitoring[notification_type]) is dict:
                            raise ValueError(f"Parameter '{notification_type}' has to be a dictionary")
                        for message_type in monitoring[notification_type]:
                            if not message_type in MESSAGE_TYPES:
                                raise ValueError(f"'{message_type}' is not a supported message type")
                        if type(monitoring[notification_type][message_type]) is str:
                            monitoring[notification_type][message_type] = [monitoring[notification_type][message_type]]
        
    def _init_regression_testing(self, regression_testing):
        self.upload_config_dir        = self._get_item(regression_testing, 'upload_config_dir', None)
        self.tests_root_dir           = self._get_item(regression_testing, 'tests_root_dir', None)
        self.use_master_settings      = self._get_item(regression_testing, 'use_master_settings', True)

    def _inputDataValidation(self):
        if self.report_logs_source and self.report_retrieve_logs: 
            raise ConfigError('Either analyze existing file or produce new ones, both is not possible')  
        if self.progress_bar and self.engine != INSPECTOR:
            self.progress_bar = False

    def get_config_keys(self):
        return [key for key in self.config_keys if not (key in self.NON_ACTION_CONFIG or key.lower().startswith("x"))]
        
    def get_offline_config_keys(self):
        return [key for key in self.get_config_keys() if key in self.OFFLINE_ACTIONS]

    def get_online_config_keys(self):
        return [key for key in self.get_config_keys() if key not in self.OFFLINE_ACTIONS]

        
class TestExecutor():

    def __init__(self, config, use_master_config_settings=True):
        self.config = config
        self.use_master_config_settings = use_master_config_settings
        
    def _execute_config(self, config_file_path):
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
        if "operational" in config:
            if self.config.version and self.use_master_config_settings:
                config["operational"]['version'] = self.config.version
        if "search" in config:
            if "search_query_file" in config["search"]:
                config["search"]['search_query_file'] = self._get_file_path(
                        config_file_path, config["search"]['search_query_file'])             
        if "reporting" in config:
            if "logs_source" in config["reporting"]:
                config["reporting"]['logs_source'] = self._get_file_path(config_file_path, config["reporting"]['logs_source']) 
        temp_config_file = join(AYFIE_EXPRESS_CFG_DIR, 'tmp.cfg')
        Os.make_dir(AYFIE_EXPRESS_CFG_DIR) 
        Os.write_to_file_as_non_root_user(temp_config_file, dumps(config).encode('utf-8'))
        try:
            sys.argv = ['ayfieExpress.py', temp_config_file]
            Admin(Config(CommandLine().get_config(False))).run_config()
        except Exception as e:
            print(f'TEST ERROR: "{config_file_path}": {str(e)}')
            raise
        
    def _retrieve_configs_and_execute(self, root_dir):
        for config_file_path in FileTools.get_next_file(root_dir, ["data", "query", "disabled", "logs"]):
            self._execute_config(config_file_path)

    def _get_file_path(self, root_path, leaf_path):
        if isabs(leaf_path):
            return leaf_path
        return join(path_split(root_path)[0], leaf_path)

    def run_tests(self):
        if self.config.upload_config_dir:
            self._retrieve_configs_and_execute(config_dir)
            config_dir = self._get_file_path(self.config.config_source, self.upload_config_dir)
        if self.config.tests_root_dir:
            config_dir = self._get_file_path(self.config.config_source, self.config.tests_root_dir)
            self._retrieve_configs_and_execute(config_dir)

    def process_test_result(self, search, result):
        if not self.config.config_source:
            self.config.config_source = f'The test'
        if type(search["result"]) is bool:
            if search["result"]:
                print(f'CORRECT: {self.config.config_source} is hardcoded to be correct')
            else:
                print(f'FAILURE: {self.config.config_source} is hardcoded to be a failure')
        elif type(search["result"]) is int:
            expected_results = int(search["result"])
            actual_results = int(result['meta']['totalDocuments'])
            query = search["query"]
            if actual_results == expected_results:
                print(f'CORRECT: The query "{query}" returns {actual_results} result(s)')
            else:
                print(f'FAILURE: The query "{query}" should return {expected_results} result(s), not {actual_results}')


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
        self.operational = Operational(self.config)
        self.installer = Installer(self.config)
        self.schema_manager = SchemaManager(self.config)
        self.feeder = Feeder(self.config, data_source)
        self.querier = Querier(self.config)
        self.document_updater = DocumentUpdater(self.config)
        self.reporter = Reporter(self.config)
        self.clusterer  = Clusterer(self.config)
        self.classifier = Classifier(self.config)
        self.doc_retriever = DocumentRetriever(self.config)
        self.sampler = Sampler(self.config)
        self.monitoring = Monitoring(self.config)
        self.uninstaller = Uninstaller(self.config)

    def _search_result_processing(self, search, result):
        result_type = type(search["result"])
        if result_type is bool or result_type is int:
            testExecutor = TestExecutor(self.config, self.config.use_master_settings)
            testExecutor.process_test_result(search, result) 
            return
        elif not search["result"] in RESULT_TYPES:
            values = '", "'.join(RESULT_TYPES)
            raise ValueError(f'Parameter "result" must be a bool, an int or one of these strings: "{values}".')
          
        output = []
        if search["result"] == NORMAL_RESULT:
            output.append(pretty_formated(result))
        elif search["result"] == QUERY_AND_HITS_RESULT:
            output.append(f'{str(result["meta"]["totalDocuments"]).rjust(8)} <== {result["query"]["query"]}')
        elif search["result"] == ID_LIST_RESULT:
            output.append(pretty_formated([doc["document"]["id"] for doc in result["result"]]))
        elif search["result"] == ID_AND_CLASSIFER_RESULT:
            search["exclude"] = ["content", "term", "location", "organization", "person", "email"]
            for page in self.querier.get_search_result_pages(search):  
                for doc in page:
                    fields = doc["document"]
                    output_line = str(fields["id"])
                    if search["classifier_field"] in fields:
                        output_line += "\t" + str(fields[search["classifier_field"]])
                    output.append(output_line)
        output = "\n".join(output)    
       
        if search["result_destination"] == RETURN_RESULT:
            return output
        elif search["result_destination"] == DISPLAY_RESULT:
            print(output)
        else:
            try:
                Os.write_to_file_as_non_root_user(search["result_destination"], output.encode('utf-8'))
            except:
                msg = f'Parameter "result_destination" must be {", ".join(RESULT_DESTINATIONS)} or a output file path'
                raise ValueError(msg)
        
    def run_config(self):
        while True:
            if self.config.regression_testing:
                TestExecutor(self.config, self.config.use_master_settings).run_tests()
                return
                
            if self.config.installer:
                try:
                    self.installer.operate()
                except EngineDown:
                    online_config_keys = self.config.get_online_config_keys()
                    if self.config.get_online_config_keys:
                        log.info(f'"{self.config.engine}" is down, unable to execute {online_config_keys}')
                        return

            if self.config.server != OFF_LINE:
                if self.config.data_source:
                    if self.config.prefeeding_action not in PRE_FEEDING_ACTIONS:
                        actions = '", "'.join(PRE_FEEDING_ACTIONS)
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
                elif not self.feeder.collection_exists():
                    if self.config.get_online_config_keys():
                        if self.config.col_name:
                            raise ConfigError(f'There is no collection "{self.config.col_name}"')
                        else:
                            actions = '", "'.join(self.config.get_online_config_keys())
                            raise ConfigError(f'The configured action(s) "{actions}" require(s) the collection to be set') 
                    
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
                if self.config.sampling:
                    pretty_print(self.sampler.create_sampling_job_and_wait())
                if self.config.doc_retrieval:
                    self.doc_retriever.retrieve_documents()
                for search in self.config.searches:
                    try:
                        result = self.querier.search(search)
                        self._search_result_processing(search, result)
                    except FileNotFoundError as e:
                        print(f'ERROR: {str(e)}')
                        return
                 
            if self.config.reporting:
                self.reporter.do_reporting()                 
            if self.config.uninstaller:
                self.uninstaller.operate()
            if self.config.monitorings:
                self.monitoring.monitor()
            if not self.config.infinite_loop:
                break


class CommandLine:

    def __init__(self, argv=None):
        if argv:
            sys.argv = argv
        self.argv = sys.argv
        pre_parser = ArgumentParser(add_help=False)
        self.parser = ArgumentParser(description='IMPORTANT: Positional arguments must come before optional arguments in spite of what usage above shows to the contarary.')
        
        generic_help = "'builtin', 'test' or path to config file"
        version_help = f"The {basename(sys.argv[0])} version"
        version_string = f"{pre_parser.prog}: {AYFIE_EXPRESS_VERSION}"
        
        pre_parser.add_argument( 'config', help=generic_help)
        self.parser.add_argument('config', help=generic_help)
        pre_parser.add_argument( '-V', '--Version', action='version', version=version_string, help=version_help)
        self.parser.add_argument('-V', '--Version', action='version', version=version_string, help=version_help)
        self.config_file_path = pre_parser.parse_known_args()[0].config
        self.file_version_count = 0
        self.conditional_count = 0
        
    def _dump_config(self, config):
        Os.make_dir(AYFIE_EXPRESS_CFG_DIR)
        path = join(AYFIE_EXPRESS_CFG_DIR, f"config_{self.file_version_count}.json")
        Os.write_to_file_as_non_root_user(path, pretty_formated(config).encode("utf-8"))
        self.file_version_count += 1
        
    def get_default_config(self):
        return """{
            "cmd_line": {
                "positional_variables": [],
                "optional_variables": {
                    "host": "localhost", 
                    "port": "80",
                    "inst_dir": "none",
                    "docker_dir": "none",
                    "docker_user": "none",
                    "docker_pwd": "none",                     
                    "version": "none",
                    "node": "single",
                    "ram": "64",
                    "primary": "", 
                    "processor": "",  
                    "secret": "none",
                    "grafana": "no",
                    "frontend": "yes",
                    "admin_page": "no",
                    "feeder": "yes",
                    "install": "no",
                    "start": "no",
                    "collection": "none",
                    "recreate": "no",
                    "data_set": "none", 
                    "data_path": "none",
                    "fuzzy": "0",
                    "loop": "no",
                    "preprocess": "none",
                    "processing": "no",
                    "clustering": "no",
                    "sampling": "no",           
                    "retrieve": "none",
                    "search":"none",
                    "size": "1",
                    "reporting": "no", 
                    "stop": "no",
                    "uninstall": "no",
                    "nuke": "no",
                    "simulate": "no"
                },
                "help_text": {
                    "host": "The ayfie Inspector API server, default: '{default}'",
                    "port": "The ayfie Inspector API port, default: '{default}'",
                    "inst_dir": "The ayfie Inspector install dir, default: the version",
                    "docker_dir": "The Docker data dir, 'none' results in default",
                    "docker_user": "The Docker registry user name",
                    "docker_pwd": "The Docker registry user password", 
                    "version": "The ayfie Inspector version number, default: '{default}'",
                    "node": "{allowed_values}, default: '{default}'",
                    "ram": "Amount of host RAM, default: '{default}'",
                    "primary": "Primary node IP/FQDN, default: '{default}'",
                    "processor": "Processor node IP/FQDN, default: '{default}'",
                    "secret": "Security client secret, default: '{default}' (no security)",
                    "grafana": "Install Grafana: {allowed_values}, default: '{default}'",
                    "frontend": "Install frontend: {allowed_values}, default: '{default}'",
                    "admin_page": "Frontend admin page: {allowed_values}, default: '{default}'",
                    "feeder": "Install feeder (Enrichor): {allowed_values}, default: '{default}'",
                    "install": "{allowed_values}, default: '{default}'",
                    "start": "Start ayfie Inspector: {allowed_values}, default: '{default}'",          
                    "collection": "The collection id/name, default: '{default}'",  
                    "recreate": "(Re)create collection: {allowed_values}, default: '{default}'",            
                    "data_set": "{allowed_values}",
                    "data_path": "Path to data file or directory, default: '{default}'",
                    "fuzzy": "Maximum random character replacments, default: '{default}'",
                    "loop": "{allowed_values}, default: '{default}'",
                    "preprocess": "ET1 only: {allowed_values}, default: '{default}'",
                    "processing": "{allowed_values}, default: '{default}'",
                    "clustering": "{allowed_values}, default: '{default}'",
                    "sampling": "{allowed_values}, default: '{default}'",
                    "retrieve": "List of fields to dump to output.csv, default: '{default}'",
                    "search": "A search query",
                    "size": "Number of returned search results, default: '{default}'",
                    "reporting": "'yes' requires version number set, default: '{default}'",
                    "stop": "Stop ayfie Inspector: {allowed_values}, default: '{default}'",
                    "uninstall": "{allowed_values}, default: '{default}'",
                    "nuke": "{allowed_values}, default: '{default}'",
                    "simulate": "Install simulation: {allowed_values}, default: '{default}'"
                },
                "allowed_values": {
                    "host": "^[0-9a-z\\\\.]+$",
                    "port": "^[0-9]{2,6}$",
                    "install": ["yes","no"],
                    "start": ["yes","no"],
                    "stop": ["yes","no"],           
                    "uninstall": ["yes","no"],
                    "version": "^(none)|([0-9\\\\.]+)$",
                    "node": ["single", "primary", "processor"],
                    "ram": "^[0-9]+$",
                    "data_set": ["single_email","smoke","smoke_fragment","none"],
                    "grafana": ["yes","no"],
                    "frontend": ["yes","no"], 
                    "admin_page": ["yes","no"],
                    "feeder": ["yes","no"],
                    "preprocess": ["IPRO","NUIX","none"],                  
                    "processing": ["yes","no"],
                    "clustering": ["yes","no"],
                    "sampling": ["yes","no"],
                    "retrieve": "^[a-zA-Z0-9,_]+$",
                    "size": "^[0-9]+$",
                    "recreate": ["yes","no"],
                    "start": ["yes","no"],
                    "stop": ["yes","no"],
                    "fuzzy": "^[0-9]+$",
                    "loop": ["yes","no"],
                    "nuke": ["yes","no"],
                    "simulate": ["yes","no"]
                }
            },
            "server": "_host_",  
            "port": "_port_",
            "job_server: _node_ != 'single' ": "_processor_",
            "col_name": "_collection_",
            "infinite_loop: _loop_ == 'yes' ": true,
            "client_secret: _secret_ != 'none' ": "_secret_",
            "installer: _install_ == 'yes' or _start_ == 'yes' ": {
                "docker_data_dir": "_docker_dir_",
                "simulation: _simulate_ == 'yes' ": true,
                "version": "_version_",
                "install_dir": "_inst_dir_",
                "enable_frontend: _node_ == 'processor' or _frontend_ == 'no' ": false,       
                "enable_grafana: _node_ != 'processor' and _grafana_ == 'yes' ": true,
                "enable_feeder: _node_ != 'processor' and _feeder_ == 'yes' ": true,
                "processor_node: _node_ == 'processor' ": true,
                "primary_node: _node_ == 'primary' ": true,
                "primary_node_IP_or_FQDN: _node_ == 'processor' ": "_primary_",
                "disable_defender": true,
                "enable_admin_page: _admin_page_ == 'yes'": true,
                "enable_gdpr": true,
                "generate_dot_env_file_only": false,
                "install: _install_ == 'yes' ": true,
                "start: _start_ == 'yes' ": true,
                "ram_on_host": "_ram_",
                "security:  _secret_ != 'none' ": {
                    "type": "self_signed",                                                   
                    "domain": "54.93.241.152",
                    "country":"DE",
                    "state":"Bayern",
                    "city":"Munich",
                    "company":"Ayfie"
                },
                "docker_registry": {
                    "user": "_docker_user_",
                    "password": "_docker_pwd_"
                }
            },
            "reporting: _reporting_ == 'yes' ": {
                "retrieve_logs": "_version_/"
            },
            "conditional: _node_ != 'processor' ": {
                "feeding: _data_set_ != 'none' or _data_path_ != 'none' ": {
                    "data_source: _data_set_ == 'smoke' ": "http://suplin1.ayfie.cloud:8000/testdata/11d.zip",
                    "data_source: _data_set_ == 'smoke_fragment' ": "http://suplin1.ayfie.cloud:8000/testdata/file0.zip",  
                    "data_source: _data_set_ == 'single_email' ": "http://suplin1.ayfie.cloud:8000/testdata/email.txt",
                    "data_source: _data_path_ != 'none' ": "_data_path_",
                    "prefeeding_action: _recreate_ == 'yes' ": "recreate_collection",
                    "no_processing": true,
                    "report_time": true,
                    "preprocess: _preprocess_ != 'none' ": "_preprocess_",
                    "ignore_csv": true,
                    "max_fuzzy_chars_per_doc": "_fuzzy_"
                },
                "processing: _processing_ == 'yes' ": {                           
                    "report_time": true
                },
                "clustering: _clustering_ == 'yes' ": {
                    "report_time": true
                },
                "sampling: _sampling_ == 'yes' ": {      
                    "method": "SMART",
                    "size": 3,
                    "smartSpecificity" : 0.3,
                    "smartDimensions" : 100,
                    "smartMinDf" : 5.0E-4,
                    "smartPresampling" : 1.0,
                    "report_time": true
                },
                "document_retrieval: _retrieve_ != 'none' " : {
                    "query": "",    
                    "fields": ["_retrieve_"],           
                    "skip_empty_documents": true,
                    "drop_underscore_fields": false
                }, 
                "search: _data_set_ == 'smoke' ": [
                    {
                        "query": "",
                        "result": 993576
                    },
                    {
                        "query": "_exists_:_et2ThreadGroupId",
                        "result": 582412
                    },
                    {
                        "query": "_duplicate:false",
                        "result": 149822
                    }
                ],
                "search: _data_set_ == 'single_email' ": [
                    {
                        "query": "*",
                        "result": 1
                    },
                    {
                        "query": "_exists_:_et2ThreadGroupId",
                        "result": 1
                    }
                ],
                "search: _search_ != 'none' ": {
                    "query": "_search_",
                    "size": "_size_"
                }
            },
            "uninstaller: _uninstall_ == 'yes' or _stop_ == 'yes' or _nuke_ == 'yes' ": {
                "simulation": false,
                "version": "_version_",
                "stop: _stop_ == 'yes' ": true,
                "uninstall: _uninstall_ == 'yes' ": true,
                "nuke_everything: _nuke_ == 'yes' ": true
            }
        }
        """ 
        
    def _read_config(self, config_file_path):
        if isfile(config_file_path):
            with open(config_file_path, 'rb') as f:
                try:
                    config = loads(f.read().decode('utf-8'))
                except JSONDecodeError as e:
                    self.parser.error(f'Config file "{config_file_path}" contains bad json: {e}')
                except Exception as e:
                    self.parser.error(f'Loading config file "{config_file_path}" failed: {e}')
        elif config_file_path == "builtin":
            config = loads(self.get_default_config())
        else:
            self.parser.error(f'"{config_file_path}" is not a valid value')
        return config
           
    def _get_help_text(self, command_line, variable):
        if "help_text" in command_line and variable in command_line["help_text"]:
            help_text = command_line["help_text"][variable]
            if "{allowed_values}" in command_line["help_text"][variable]:
                if variable in command_line["allowed_values"]:
                    values = "', '".join(command_line["allowed_values"][variable])
                    help_text = help_text.replace("{allowed_values}", f"'{values}'")
            if "{default}" in command_line["help_text"][variable]:
                help_text = help_text.replace("{default}", command_line["optional_variables"][variable])
            return help_text
        return "config file defined argument"
       
    def get_config(self, is_run_from_cmd_line): 
        config = self._read_config(self.config_file_path) 
        self._dump_config(config)
        if "include" in config:
            config_to_include = self._read_config(config['include'])
            del config["include"]
            config = {**config, **config_to_include}
        if "cmd_line" in config:
            command_line = config["cmd_line"]
            del config["cmd_line"]
            self.parser.add_argument('-s', '--show', help="No execution, just shows settings (including defaults)", action='store_true') 
            if "positional_variables" in command_line:
                for variable in command_line["positional_variables"]:
                    self.parser.add_argument(f'{variable}', help=self._get_help_text(command_line, variable), action='store')
            if "optional_variables" in command_line:
                for variable in command_line["optional_variables"]:
                    self.parser.add_argument(f'--{variable}', help=self._get_help_text(command_line, variable), action='store', 
                                             nargs="?", default=command_line["optional_variables"][variable], metavar="value")
            if "allowed_values" in command_line:
                args = self.parser.parse_args()
                for arg in command_line["allowed_values"]:
                    if arg in vars(args):
                        value = getattr(args, arg)
                        allowed_values = command_line["allowed_values"][arg]
                        if not value:
                            self.parser.error(f"Parameter '{arg}' has not been given a value.")
                        err_msg = f"Value error, '{value}' is not a valid value for '{arg}'."
                        if type(allowed_values) is list:
                            if not value in command_line["allowed_values"][arg]:
                                valid_values = "', '".join(command_line["allowed_values"][arg])
                                err_msg += f" Valid values are: '{valid_values}'"
                                self.parser.error(err_msg)
                        else:
                            if not match(allowed_values, value):
                                err_msg += f" Value have to match regex: '{allowed_values}'"
                                self.parser.error(err_msg)
        args = vars(self.parser.parse_args())
        if "show" in args:
            if args["show"]:
                print("Parameter values (including default values):")
                pretty_print(args)
                exit()
            del args["show"]
        config = self._process_conditional_keys(config, args)
        self._dump_config(config)
        config = self._right_shift_remaining_conditional_parameters(config) 
        self._dump_config(config)        
        config = self._replace_variables(config, args)
        if 'config_source' not in config:
            config['config_source'] = self.config_file_path
        if not 'silent_mode' in config:
            if is_run_from_cmd_line:
                config['silent_mode'] = False
            else:
                config['silent_mode'] = True
        config = loads(dumps(config).replace('"none"', 'null'))
        config["cmdline"] = ' '.join(self.argv)
        self._dump_config(config)
        return config
        
    def _process_key(self, struct, args, key):
        keyparts = key.split(":")
        if len(keyparts) == 2:
            boolean_expression = keyparts[1]
            for arg in args:
                boolean_expression = boolean_expression.replace(f"_{arg}_", f"'{args[arg]}'")
            try:
                if eval(boolean_expression):
                    new_key = keyparts[0]
                    if new_key == CONDITIONAL_KEY:
                        new_key = f"{new_key}_{str(self.conditional_count)}"
                        self.conditional_count += 1
                    struct[new_key] = struct[key]
            except NameError as e:
                self.parser.error(f"Error in conditional expression ({keyparts[1]}): {str(e)}")
            del struct[key]
        return struct
        
    def _process_conditional_keys(self, struct, args):
        if type(struct) is dict:
            for key in list(struct.keys()):
                struct = self._process_key(struct, args, key)
            for key in list(struct.keys()):
                struct[key] = self._process_conditional_keys(struct[key], args)
        elif type(struct) is list:
            for i, item in enumerate(struct):
                struct[i] = self._process_conditional_keys(item, args)
        return struct
        
    def _right_shift_remaining_conditional_parameters(self, struct):
        if type(struct) is dict:
            for key in list(struct.keys()):
                if key.startswith(CONDITIONAL_KEY): 
                    for sub_key in struct[key]:
                        struct[sub_key] = struct[key][sub_key]
                    del struct[key]
                else:
                    struct[key] = self._right_shift_remaining_conditional_parameters(struct[key])
        elif type(struct) is list:
            for i, item in enumerate(struct):
                struct[i] = self._right_shift_remaining_conditional_parameters(item)
        return struct
        
    def _replace_variables(self, struct, variables):
        if not variables:
            return struct
        if type(struct) is dict:
            for key in struct.keys():
                struct[key] = self._replace_variables(struct[key], variables)
        elif type(struct) is list:
            for i in range(len(struct)):
                struct[i] = self._replace_variables(struct[i], variables)
        elif type(struct) is str:
            for key in variables.keys():
                struct = struct.replace(f"_{key}_", variables[key])
        return struct

 
class AyfieExpressRegressionTest:

    def __init__(self):
        self.versions = [
                "2.1.0", "2.2.0", "2.3.0", "2.4.1", "2.4.2", "2.5.0", "2.5.1", "2.6.0", "2.6.1", "2.7.0", 
                "2.8.0", "2.8.1", "2.8.2", "2.9.0", "2.9.1", "2.10.0", "2.11.0", "2.12.0", "2.12.1"
             ]
        self.current_customer_versions = ["2.5.0", "2.6.1", "2.9.1"]
        self.system_test_targets = ['default', 'all', 'historical', 'customer', 'security', 'memory', 'fuzzy',  'mini_smoke', 'latest']
        self.unit_test_targets = ['default', 'all']
        
    def get_test_targets(self, test_type):
        if test_type == 'unit':
            return self.unit_test_targets
        elif test_type == 'system':
            return self.system_test_targets
        raise ValueError(f"There is no test type '{test_type}'")
        
    def run_tests(self, test_type, test_target, docker_user=None, docker_password=None, client_secret=None):
        if test_type == 'system':
            self.run_end_to_end_tests(test_target, docker_user, docker_password, client_secret)
        elif test_type == 'unit':
            self.run_unit_tests(test_target)

    def _print_test_heading(self, test_target, version):
        print( f"\n##### Version {version} tested end to end for test target '{version}' #####")
        
    def _get_command_line(self, version, data_set, docker_user, docker_password, run_processing=True, 
                          enable_all_features=False, fuzzy_testing=False, secret=None, ram=64):
        cmdline = f"ayfieExpress.py builtin --install yes --start yes --port 20000 --version {version} --data_set {data_set}"
        cmdline += " --collection regression --recreate yes --uninstall yes --preprocess IPRO"
        cmdline += f" --docker_user {docker_user} --docker_pwd {docker_password}"
        if HOST_OS == OS_LINUX:
            cmdline += f" --docker_dir /home/docker "
        if run_processing:
            cmdline += f" --processing yes"
        if enable_all_features:
            cmdline += " --clustering yes --sampling yes --grafana yes --feeder yes"
            cmdline += " --reporting yes --retrieve creditcard,iban --admin_page yes"  
        if fuzzy_testing:
            cmdline += " --fuzzy yes --loop yes"
        if secret:
            cmdline += f" --secret {secret}"
        if ram != 64:
            cmdline += f" --ram {ram}"
        return cmdline
        
    def _display_command_line(self, cmdline):
        python = "python"
        if HOST_OS == OS_LINUX:
            python = "python3"
        print(f"{python} {cmdline}")
        
    def _execute_test(self, test_target, version, data_set, docker_user, docker_password, run_processing=True, 
                      enable_all_features=False, fuzzy_testing=False, secret=None, ram=64):
        self._print_test_heading(test_target, version)
        cmdline = self._get_command_line(version, data_set, docker_user, docker_password, run_processing, 
                                         enable_all_features, fuzzy_testing, secret, ram)
        self._display_command_line(cmdline)
        Admin(Config(CommandLine(cmdline.split()).get_config(True))).run_config() 
    
    def run_end_to_end_tests(self, test_target, docker_user=None, docker_password=None, client_secret=None):
        if test_target in ['historical', 'all']:
            for version in self.versions: 
                self._execute_test('historical', version, "single_email", docker_user, docker_password, enable_all_features=False)
        if test_target in ['customer', 'all']:
            for version in self.current_customer_versions:
                self._execute_test('customer', version, "smoke_fragment", docker_user, docker_password, enable_all_features=True) 
        if test_target in ['default', 'latest', 'all']:
            self._execute_test('latest', self.versions[-1], "smoke", docker_user, docker_password, enable_all_features=True)
        if test_target in ['mini_smoke']:
            self._execute_test('latest', self.versions[-1], "single_email", docker_user, docker_password, enable_all_features=True)
        if test_target in ['memory', 'all']:
            for memory in ['32']:
                self._execute_test('memory', self.versions[-1], "smoke", docker_user, docker_password, enable_all_features=True, ram=memory)             
        if test_target in ['fuzzy', 'all']:
            self._execute_test('fuzzy', self.versions[-1], "smoke", docker_user, docker_password, 
                               run_processing=False, enable_all_features=True, fuzzy_testing=True)
        # 'security' is not included in 'all' as it involves manual steps               
        if test_target in ['security']:
            self._execute_test('security', self.versions[-1], "single_email", docker_user, docker_password, 
                                enable_all_features=True, security_testing=True)  

    def run_unit_tests(self, test_type=None, simulate=True):
        self.test_Os_class()
        self.test_Docker_class(simulate)

    def test_Os_class(self):
        DIRECTORY    = "test_xyz"
        USER         = "test_user_xyz"
        PASSWORD     = "password_xyz"
        GROUP        = "test_group_xyz"
        MODE         = "u+x"
        SCRIPT_NAME  = f"script_xyz.{Os.get_script_extension()}"
        WINDOWS_CODE = f"dir {SCRIPT_NAME}"
        LINUX_CODE   = f"#!/bin/bash\nls -al {SCRIPT_NAME}"

        if HOST_OS == OS_LINUX:
            SCRIPT_CODE = LINUX_CODE
        elif HOST_OS == OS_WINDOWS:
            SCRIPT_CODE = WINDOWS_CODE
        
        linux_distro, version = Os.get_linux_distro_and_version()
        assert(HOST_OS == OS_WINDOWS or linux_distro in LINUX_DISTROS), f"'{linux_distro}' is an unsupported Linux distro"
        assert(HOST_OS == OS_WINDOWS or version != None), "Version should  not be None for a Linux distro"
        assert (Os.get_actual_user() in Os.get_users()), f"The actual user is not among existing users."
        if Os.user_exists(USER):
            Os.delete_user(USER)
        assert (not USER in Os.get_users()), f"User '{USER}' should not be a user."
        try:
            Os.create_user(USER, PASSWORD)
            assert (HOST_OS == OS_WINDOWS or (HOST_OS == OS_LINUX and Os.user_exists(USER))), f"Failed to crete user '{USER}'"
        except PermissionError as e:
            if HOST_OS == OS_WINDOWS or (HOST_OS == OS_LINUX and Os.is_root_or_elevated_mode()):
               raise
        try:
            if Os.group_exists(GROUP):
                Os.delete_group(GROUP)
            assert (not GROUP in Os.get_groups()), f"Group '{GROUP}' should not be a group."
        except NotImplementedError as e:
            assert (HOST_OS == OS_WINDOWS), f"Failed in regard to group '{GROUP}'"
        try:
            Os.create_group(GROUP)
            assert (HOST_OS == OS_WINDOWS or (HOST_OS == OS_LINUX and Os.group_exists(GROUP))), f"Failed to crete group '{GROUP}'"
            Os.add_user_to_groups(USER, GROUP)
        except PermissionError as e:
            if HOST_OS == OS_WINDOWS or (HOST_OS == OS_LINUX and Os.is_root_or_elevated_mode()):
               raise
        Os.make_dir(DIRECTORY)
        with change_dir(DIRECTORY):
            Os.write_to_file_as_non_root_user(SCRIPT_NAME, SCRIPT_CODE.encode("utf-8"))
            Os.change_mode(SCRIPT_NAME, MODE)
            if Os.is_root_or_elevated_mode():
                Os.change_owner(SCRIPT_NAME, USER)
            output = execute(join(".", SCRIPT_NAME), return_response=True)
            assert (SCRIPT_NAME in output), "Unexpected script output"
        try:
            Os.delete_group(GROUP) 
            assert (not Os.group_exists(GROUP)), f"Failed to delete group '{GROUP}'" 
        except NotImplementedError as e:
            assert (HOST_OS == OS_WINDOWS), f"Failed to delete or check for existence of group '{GROUP}'"    
        if Os.is_root_or_elevated_mode():
            Os.delete_user(USER)
        try:
            assert (not Os.user_exists(GROUP)), f"Failed to delete group '{GROUP}'"
        except NotImplementedError as e:
            assert (HOST_OS == OS_WINDOWS), f"Failed to delete or check for existence of group '{GROUP}'"    
        Os.recreate_dir(DIRECTORY) 
        with change_dir(DIRECTORY):
            assert (len(listdir()) == 0), f"Failed to recreate directory {DIRECTORY}" 
        assert (DIRECTORY in listdir()), f"Expected to find directory {DIRECTORY}" 
        Os.del_dir(DIRECTORY)
        assert (not DIRECTORY in listdir()), f"Did not expect to find directory {DIRECTORY} in existence" 
        print("SUCCESSFUL testing of Class Os")
        
    def _test_Docker_class(self, simulate):
        simulation = {}
        if simulate:
            simulation = {
            'installed': False, 
            'running': False, 
            'os': OS_WINDOWS,
            'linux_distro': None
        }
        docker = Docker(simulate, simulation)  
        docker.install()
        docker.start()
        dockerCompose = DockerCompose(simulate, simulation)
        docker.stop()
        docker.uninstall()
        docker.nuke_everything()
        print("SUCCESSFUL testing of Class Docker")
        
    def test_Docker_class(self, simulate=True):
        if simulate:
            os_flavors = [(OS_WINDOWS, None)]
            for distro in DOCKER_LINUX_DISTROS:
                os_flavors.append((OS_LINUX, distro))
            for os_flavor in os_flavors:
                simulation = {}
                if simulate:
                    simulation = Docker.gen_simulation_config()
                    simulation['os'] = os_flavor[0]
                    simulation['linux_distro'] = os_flavor[1]
                    simulation["installed"] = True
                    simulation["running"] = True
                print(f"\n\n------ Testing {os_flavor} with simualation ON ------\n")
                self._test_Docker_class(simulation)
        else:
            print(f"\n------ Testing with simualation OFF ------\n") 
            self._test_Docker_class()   

def test_help(prog_name, test_type, test_target, test_targets):
    if not test_type:
        print(f"usage: {prog_name} test <type> <target> [<user>] [<password>] [<secret>]")
    elif test_type == 'unit':
        print(f"usage: {prog_name} test {test_type} <target>")
    else:
        target = "<target>"
        if test_target:
            target = test_target
        if test_type == 'system':
            cmdline = f"usage: {prog_name} test {test_type} {target} <user> <password>"
            if test_target == 'security':
                print(f"{cmdline} <secret>")
            else:
                print(f"{cmdline} [<secret>]")
        else:
            print(f"{prog_name}: error: '{test_type}' is not a valid test type")
            print(f"usage: {prog_name} test <type> {target} [<user>] [<password>] [<secret>]")
    print("\npositional arguments:")
    if test_targets:
        print("  target     '" + "','".join(test_targets) + "'")
    else:
        print("  type       'unit' or 'system'")                
        print("  target     Run 'ayfieExpress.py test <type> -h'")
    if test_type != 'unit':
        print("  user       Docker registry user name")                
        print("  password   Docker registry user password")
        print("  secret     Keycloak client secret")
        
def tool_help(prog_name):
    print(f"usage: {prog_name} <mode> [-h]")
    print("\npositional arguments:")
    print("  mode            'builtin', 'test' or a config file path")
    print("\noptional arguments:")
    print("  -h, --help      Help details in regard to the selected mode") 
  
  
def main():
    numb_of_args = len(sys.argv)
    prog_name = sys.argv[0]
    help_options = ['-h', '--help']
    if numb_of_args == 1 or (numb_of_args == 2 and sys.argv[1] in help_options):
        tool_help(prog_name)      
    elif numb_of_args >= 2: 
        if sys.argv[1] == 'test':
            test_type = test_target = user = password = client_secret = None 
            if numb_of_args >= 3:
                test_type = sys.argv[2]
                if test_type in help_options:
                    test_help(prog_name, None, None, None)
                    return
            if numb_of_args >= 4:
                test_target = sys.argv[3]
            if numb_of_args >= 5:
                user = sys.argv[4]
            if numb_of_args >= 6:
                password = sys.argv[5]
            if numb_of_args >= 7:
                client_secret = sys.argv[6]
            if not test_type:
                test_help(prog_name, None, None, None)
            elif not test_type in ['unit','system']:
                test_help(prog_name, test_type, None, None)
            elif test_target:
                test_targets = AyfieExpressRegressionTest().get_test_targets(test_type)
                if not test_target in test_targets:
                    print(f"{prog_name}: error: '{test_target}' is not a valid {test_type} test target")
                    test_help(prog_name, test_type, None, test_targets)
                elif test_target == 'security' and user and password and not client_secret:
                    print(f"{prog_name}: error: 'security' requires client secret to be set")
                    test_help(prog_name, test_type, test_target, test_targets)
                else:
                    if test_type == 'system':
                        if user and password:
                            AyfieExpressRegressionTest().run_tests(test_type, test_target, user, password, client_secret)
                        else:
                            print(f"{prog_name}: error: Docker registry user and password not set")
                            test_help(prog_name, test_type, test_target, test_targets)
                    if test_type == 'unit':
                        AyfieExpressRegressionTest().run_tests(test_type, test_target)
            else:
                test_targets = AyfieExpressRegressionTest().get_test_targets(test_type)
                test_help(prog_name, test_type, test_target, test_targets)
        else:
            Admin(Config(CommandLine().get_config(True))).run_config() 
                   
if __name__ == '__main__':
    main()

          
# The next line is required by the powershell script on the first few lines{secret}
#>
