import pymongo
import traceback
import time
from pymongo.errors import OperationFailure

class MongoManager(object):

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.tick_data_csv

    def _create_index(self, column_name, collection):
        collection.create_index(column_name, name = 'index_' + column_name)

    def _drop_index(self, index_name, collection):
        # if not index_name.startswith('index_'):
        #     index_name = 'index_' + index_name
        collection.drop_index(index_name)

    def main_for_a_collection(self, collection, drop=True):
        if drop:
            self._drop_index('sym_1_exp_d_1_ts_1', collection)
            print("Dropped index for sym_1_exp_d_1_ts_1")
        available_indexes = [ind[6:] for ind in list(collection.index_information().keys()) if ind.startswith('index_')]
        for name in ['sym', 'exp_d', 'ts']:
            if name in available_indexes:
                print("Index for {} already exists".format(name))
                continue
            print("Creating index for " + name)
            self._create_index(name, collection)

    def main_for_all_collections(self):
        collections = self.db.collection_names()
        total_collections = len(collections)
        for i, collection in enumerate(collections):
            print("Starting for collection {}/{}".format(i+1, total_collections))
            start = time.time()
            try:
                print("Starting for {}".format(collection))
                collection = self.db[collection]
                self.main_for_a_collection(collection)
            except OperationFailure as e:
                self.main_for_a_collection(collection, drop=False)
            print("Finished for {} in {} seconds".format(collection, time.time() - start))


if __name__ == '__main__':
    mongo_manager = MongoManager()
    mongo_manager.main_for_all_collections()

