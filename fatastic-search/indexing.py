import json
import requests
import settings


class Resource(object):
    def __init__(self):
        self.data = self.get_data()

    def get_data(self):
        # Calling api in the future
        return self.load_data()

    @staticmethod
    def load_data():
        with open("test_data.json") as json_file:
            json_data = json.load(json_file)
        return json_data


class IndexerFramework(object):
    def __init__(self):
        self.indexer = settings.ELASTIC_SEARCH_ENDPOINT
        self.index = ''
        self.type = ''
        self._indexing()

    def _indexing(self):
        return

    def _get_index(self):
        return '/'.join([self.indexer, self.index, self.type])


class ZipCodeIndexer:
    def __init__(self, json_data):
        #IndexerFramework.__init__(self)
        self.data = json_data
        self.indexer = settings.ELASTIC_SEARCH_ENDPOINT
        self.index = 'properties'
        self.type = 'zipcode'
        self._indexing()

    def _get_index(self):
        return '/'.join([self.indexer, self.index, self.type])

    def _indexing(self):
        index = self._get_index()
        try:
            for d in self.data:
                address = d.get('address')
                url = '/'.join((index, address.get('postalCode')))
                response = requests.put(url, data=json.dumps(d))
                print(response)
        except ValueError as e:
            print ("Error: %s" % e)


class ClientIndexer(IndexerFramework):
    def __init__(self, json_data):
        super(ClientIndexer, self).__init__()
        self.data = json_data
        self._indexing()

    def _indexing(self):
        pass


def main():
    resource = Resource()
    ZipCodeIndexer(resource.data)


if __name__ == '__main__':
    main()




