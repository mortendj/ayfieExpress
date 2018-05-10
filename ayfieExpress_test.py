from ayfieExpress import Ayfie, pretty_print, HTTPError, run_cmd_line, pretty_print
import unittest
import logging
from os.path import basename, join
from sys import argv
import json


class SetUpException(Exception):
    pass


TEST_SERVER      = 'support1.ayfie.cloud'
TEST_PORT        = '21120'  # Be aware that all collections will be deleted
TEST_API_VERSION = 'v1'
LOG_DIR          = 'log'
CONFIG_DIR       = 'config'
INPUT_DATA_DIR   = 'inputData'


class BaseTestClass(unittest.TestCase):

    def _get_some_simple_test_data(self):
        content = "It seems that one need more text in order to be able to do this. I will "
        content += "add more text just to increase the likelihood of this being detected as "
        content += "English and that something start happening. So far nothing seems to work. "
        content += "This is some simple text that will do the job as content for document"
        query = content
        documents = [
            {"id": "000001", "fields": {"content": content + " 1"}},
            {"id": "000002", "fields": {"content": content + " 2"}}
        ]
        return query, documents

    def _delete_everything(self):
        self.ayfie = Ayfie(server=TEST_SERVER, port=TEST_PORT)
        self.ayfie.delete_all_classifiers()
        self.ayfie.delete_all_collections_and_wait()
        if len(self.ayfie.get_collection_names()) > 0:
            raise SetUpException
        if len(self.ayfie.get_classifier_names()) > 0:
            raise SetUpException
            
    def _create_and_build_index_for_N_collections(self, N):
        query, documents = self._get_some_simple_test_data()
        col_ids = []
        for n in range(N):
            col_id = self.ayfie.create_collection_and_wait(f'col_{n}')
            self.ayfie.feed_and_process_collection_and_wait(col_id, documents)
            col_ids.append(col_id)
        return col_ids
    

class DeleteWhatIsNotThereTestCases(BaseTestClass):

    def setUp(self):
        self._delete_everything()
        self.name = 'test_collection'

    def test_delete_non_existing_collections(self):
        self.ayfie.delete_all_collections_and_wait()
        self.assertEqual(self.ayfie.get_collection_names(), [])
        self.assertEqual(self.ayfie.get_collection_ids(), [])

    def test_delete_non_existing_collection(self):
        self.assertRaises(HTTPError, self.ayfie.delete_collection, self.name)
        self.assertRaises(HTTPError, self.ayfie.delete_collection_and_wait, self.name)

    def test_delete_non_existing_classifiers(self):
        self.ayfie.delete_all_classifiers()
        self.assertEqual(len(self.ayfie.get_classifier_names()), 0)

    def test_transitions_for_non_existing_collection(self):
        self.assertRaises(HTTPError, self.ayfie.commit_collection, self.name)
        self.assertRaises(HTTPError, self.ayfie.process_collection, self.name)


class DoSomethingWithEmptyCollectionTestCases(BaseTestClass):

    def setUp(self):
        self._delete_everything()
        self.col_name = 'test_collection'
        self.col_id = self.ayfie.create_collection_and_wait(self.col_name)
        if self.col_name not in self.ayfie.get_collection_names():
            raise SetUpException
        if not self.ayfie.exists_collection_with_id(self.col_id):
            raise SetUpException
            
    def __confirmTransition(self, col_name, query, exp_state, exp_result):
        state = self.ayfie.get_collection_state(col_name)
        self.assertEqual(state, exp_state)
        numberOfQueryHits = len(self.ayfie.search_collection(col_name, query))
        self.assertEqual(numberOfQueryHits, exp_result)

    def test_delete_existing_collection_and_wait(self):
        self.ayfie.delete_collection_and_wait(self.col_id)
        self.assertTrue(self.col_name not in self.ayfie.get_collection_names())
        self.assertTrue(self.col_id not in self.ayfie.get_collection_ids())

    def test_create_collection(self):
        col_id = self.ayfie.create_collection(self.col_name)
        self.assertEqual(len(self.ayfie.get_collection_ids(self.col_name)), 2)

    def test_create_collection_and_wait(self):
        col_id = self.ayfie.create_collection_and_wait(self.col_name)
        self.assertEqual(len(self.ayfie.get_collection_ids(self.col_name)), 2)

    def test_failing_transitions(self):
        self.assertRaises(HTTPError, self.ayfie.commit_collection, self.col_id)
        self.assertRaises(HTTPError, self.ayfie.process_collection, self.col_id)

    def test_feed_and_process_and_search_individual_functions(self):
        query, documents = self._get_some_simple_test_data()
        numbOfDocs = len(documents)
        self.assertTrue(self.col_name in self.ayfie.get_collection_names())
        self.assertTrue(self.col_id in self.ayfie.get_collection_ids())
        self.__confirmTransition(self.col_id, query, 'EMPTY', 0)
        self.ayfie.feed_collection_documents(self.col_id, documents)
        self.__confirmTransition(self.col_id, query, "IMPORTING", 0)
        self.ayfie.commit_collection_and_wait(self.col_id, timeout=300)
        self.__confirmTransition(self.col_id, query, "COMMITTED", numbOfDocs)
        self.ayfie.process_collection_and_wait(self.col_id, timeout=300)
        self.__confirmTransition(self.col_id, query, "PROCESSED", numbOfDocs)

    def test_feed_and_process_collection_function(self):
        query, documents = self._get_some_simple_test_data()
        self.ayfie.feed_and_process_collection_and_wait(self.col_id, documents)
        self.__confirmTransition(self.col_id, query, "PROCESSED", len(documents))


class CreateAndProcessCollectionTestCases(BaseTestClass): 

    def setUp(self):
        self._delete_everything()
        self.name = 'test_collection'

    def test_create_collection_and_wait_then_verify(self):
        col_id = self.ayfie.create_collection_and_wait(self.name)
        collection_name = self.ayfie.get_collection(col_id)['name']
        self.assertEqual(collection_name, self.name)
        self.assertEqual(self.ayfie.get_collection_state(col_id), 'EMPTY')
        self.assertEqual(self.ayfie.get_collection_size(col_id), 0)
        schema = self.ayfie.get_collection_schema(col_id)
        self.assertEqual(sorted([top_field for top_field in schema]),
                         sorted(list(map(str, ['fields', '_links', 'name']))))
        self.assertTrue(self.name in self.ayfie.get_collection_names())


class JobTestCases(BaseTestClass):

    def setUp(self):
        self._delete_everything()

    def __get_similarity_job_config(self, col_id, min_similarity):
        return {
            "collectionId": col_id,
            "type": "PROCESSING",
            "settings" : {
                "processing" : {
                    "nearDuplicateEvaluation" : {
                        "minimalSimilarity": min_similarity
                    }
                }
            }
        }  

    def txest_near_duplicate_job(self):
        col_id = self._create_and_build_index_for_N_collections(1)[0]
        for min_similarity in [1, 100]:
            job_config = self.__get_similarity_job_config(col_id, 100)
            self.ayfie.create_job_and_wait(job_config)
            pretty_print(self.ayfie.search_collection(col_id, '*'))

    def txest_added_collection_schema_field(self):
        field_name = 'testField'
        self.ayfie.add_collection_schema_field(self.col, field_name,
                                               'TEXT_EN')
        #print(self.ayfie.get_collection_schema_field(self.col, field_name)


    def txest_clustering_before_processing_completed(self):
        query, documents = self._get_some_simple_test_data()
        settings = {
            "clustering" : {
                "clustersPerLevel" : 20,
                "documentsPerCluster" : 30,
                "maxRecursionDepth" : 2,
                "gramianSingularVectors" : 50,
                "minDf" : 3,
                "maxDfFraction" : 0.25,
                "gramianThreshold" : 0.14
            }
        }  
        self.ayfie.feed_and_process_collection_and_wait(self.col, documents)
        self.__confirmTransition(self.col, query, "PROCESSED", len(documents)) 
        self.ayfie.create_job("CLUSTERING", self.col, settings, [],
                              "custom_cluster_field")

"""
    def test_create_clustering_job_and_wait(self):
        query, documents = self._get_some_simple_test_data()
        self.ayfie.feed_and_process_collection_and_wait(self.col_id, documents)
        self.__confirmTransition(self.col_id, query, "PROCESSED", len(documents)) 
        self.ayfie.create_clustering_job_and_wait(self.col_id, min_df=3)

    def test_failing_transitions(self):
        self.assertRaises(HTTPError, self.ayfie.commit_collection, self.name)
        self.assertRaises(HTTPError, self.ayfie.process_collection, self.name)

other

    def test_delete_all_collections(self):
        self._create_and_build_index_for_N_collections(2)
        self.assertEqual(len(self.ayfie.get_collection_names()), 2)
        self.assertEqual(len(self.ayfie.get_collection_ids()), 2)
        self.ayfie.delete_all_collections_and_wait()
        self.assertEqual(self.ayfie.get_collection_names(), [])
        self.assertEqual(self.ayfie.get_collection_ids(), [])


classification

    def test_delete_all_classifiers(self):
        N = 2
        training_field = 'training_field'
        min_score = 0.6
        num_results = 2
        for col_name in self._create_and_build_index_for_N_collections(N):
            classifier_name = f'{col_name}_classifier'
            self.ayfie.create_classifier(classifier_name, col_name, training_field,
                          min_score, num_results)
        self.assertEqual(len(self.ayfie.get_classifier_names()), N)
        self.ayfie.delete_all_classifiers()
        self.assertEqual(len(self.ayfie.get_classifier_names()), 0)
"""

        
class AdminConfigurationTestCases(unittest.TestCase):

    def setUp(self):
        self.data_path   = join(INPUT_DATA_DIR, 'testData')
        self.config_path = join(CONFIG_DIR, 'testConfig')

    def __write_to_file(self, path, content):
        if not type(content) is str:
            content = json.dumps(content)
        with open(path, 'wb') as f:
            f.write(content.strip().encode('utf-8'))

    def __run_admin_test(self, config, data=None):
        if data:
            self.__write_to_file(self.data_path, data)
        self.__write_to_file(self.config_path, config)
        return run_cmd_line(['ayfieExpress.py', self.config_path])

    def txest_complete_config(self):
        data = '\n'.join([
            "column1;column2;column3",
            "some_id;some data;some more data",
        ])
        config = {
            "server": TEST_SERVER,
            "port": TEST_PORT,
            "api_version": TEST_API_VERSION,
            "col_name": "complete_config_test",
            "feeding": {    
                "data_type": "csv",         
                "deletion": "collection",
                "format": {"delimiter": ";"},
                "encoding": "iso-8859-1",
                "batch_size": 10000,
                "data_file": self.data_path,
                "data_dir": None,
                "csv_mappings" : {
                    "id": "column1",
                    "fields": {
                        "content": "column3"
                    }
                }
            },
            "classification": {
                "col_name": "some collection name",    # Only needed if different from above
                "training_column": "some_csv_column",
                "training_split": 90,
                "classifier_name": "some_classifier_name",
                "classifierId": "some_classifier_name",
                "training_field": "trainingClass",
                "min_score": 0.6,
                "num_results": 2,
                "output_field": "pathclassification",
                "filters": []
            },
            "clustering": {
                "col_name": "some collection name",    # Only needed if different from above
                "clustersPerLevel" : 20,
                "documentsPerCluster" : 1000,
                "maxRecursionDepth" : 2,
                "gramianSingularVectors" : 50,
                "minDf" : 3,
                "maxDfFraction" : 0.25,
                "gramianThreshold" : 0.14,
                "filters" : [],
                "outputField" : "_cluster"
            },
            "search": {
                "col_name": "some collection name",    # Only needed if different from above
                "search_result": "display",            # "display", "return", "<expected number of results>" 
                "highlight" : True,
                "sort_criterion": "_score",
                "sort_order": "desc",
                "minScore": 0.0,
                "exclude": [],                        # "content", "term", "location",....
                "aggregations": [],                   # "_cluster",....
                "filters": [],
                "query": "*",
                "scroll": False,
                "size": 10,
                "offset": 0
            }
        }
        result = self.__run_admin_test(config, data)
        self.assertEqual(int(result['meta']['totalDocuments']), 1)

    def test_hello_world_example(self):
        data = {
            "documents": [
                {  
                    "id": "hello_doc_id",
                    "fields": {
                        "content": "Hello World!"
                    }
                }
            ]
        }
        config = {
            "server": TEST_SERVER,
            "port": TEST_PORT,
            "col_name": "hello_world",
            "feeding": {
                "data_file": self.data_path,
                "prefeeding_action": "recreate_collection"
            },
            "search": {
                "result": "return",
                "query": "hello"
            }
        }
        result = self.__run_admin_test(config, data)
        self.assertEqual(int(result['meta']['totalDocuments']), 1)

    def test_feeding_many_file_formats(self):
        config = {
            "server": TEST_SERVER,
            "port": TEST_PORT,
            "col_name": "file_formats",
            "feeding": {
                "data_file": "./testFiles", 
                "prefeeding_action": "create_collection_if_not_exists"
            },
            "search": {
                "result": "return",
                "query": "Værøy"
            }
        }
        result = self.__run_admin_test(config)
        self.assertEqual(int(result['meta']['totalDocuments']), 12)

    def txest_clustering_example(self):
        data = {
            "documents": [
                {  
                    "id": "hello_doc_id",
                    "fields": {
                        "content": "Hello World!"
                    }
                }
            ]
        }
        config = {
            "server": TEST_SERVER,
            "port": TEST_PORT,
            "col_name": "cluster_test",
            "data_type": "csv",    
            "deletion": "collection",
            "format": {
                "delimiter": ";"
            },
            "encoding": "iso-8859-1",
            "data_file": self.data_path,
            "search_result": "return",
            "search": {
                "highlight" : True,
                "exclude": ["content"],
                "aggregations": ["_cluster"],
                "query": "*",
                "size": 100,
         
            },
            "clustering": {
                "collectionId" : "cluster_test",
                "type" : "CLUSTERING",
                "settings" : {
                        "clustering" : {
                        "clustersPerLevel" : 20,
                        "documentsPerCluster" : 1000,
                        "maxRecursionDepth" : 2,
                        "gramianSingularVectors" : 50,
                        "minDf" : 3,
                        "maxDfFraction" : 0.25,
                        "gramianThreshold" : 0.14
                    }
                },
                "filters" : [],
                "outputField" : "_cluster"
            },
            "field_mappings" : {
                "id": "docid",
                "fields": {
                    "content": "hello"
                }
            }
        }
        result = self.__run_admin_test(config, data)
        self.assertEqual(int(result['meta']['totalDocuments']), 1)


    def txest_classification_example(self):
        data = '\n'.join([
            "id;category;content",
            "01;color;A brown house",
            "02;color;A yellow bird",
            "03;no-color;A fast car",
            "04;color;My red bike",
            "05;color;The green trees",
            "06;no-color;Under the table",
            "07;no-color;Behind the house",
            "08;color;All the red buses",
            "09;no-color;Around the next corner",
            "10;color;The red Ferrari"
        ])
        config = {
            "server": test_server,
            "port": TEST_PORT,
            "col_name": "classifier_test",
            "data_type": "csv",    
            "deletion": "collection",
            "format": {
                "delimiter": ";"
            },
            "encoding": "utf-8",
            "data_file": self.data_path,
            "search_result": "return",
            "search": {
                "query": "*",
            },
            "training_column": "category",
            "classification": {
                "collectionId" : "classifier_test",
                "classifier_name": "my_classifier",
                "classifierId": "my_classifier",
                "training_field": "trainingClass",
                "min_score": 0.6,
                "num_results": 2,
                "output_field": "pathclassification",
                "filters": [],
            },
            "field_mappings" : {
                "id": "id",
                "fields": {
                    "content": "content"
                }
            }
        }
        result = self.__run_admin_test(config, data)
        self.assertEqual(int(result['meta']['totalDocuments']), 10)


if __name__ == '__main__':
    logging.basicConfig(
            format = '%(asctime)s %(levelname)s: ' +
                     '%(message)s, %(filename)s(%(lineno)d)',
            datefmt = '%m/%d/%Y %H:%M:%S',
            filename = join(LOG_DIR, basename(argv[0]).split('.')[0] + ".log"),
            level = logging.DEBUG)
    unittest.main()
