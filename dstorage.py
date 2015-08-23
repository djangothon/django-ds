# storage and fetch logic goes here

import constants

from django.core.files.storage import Storage
from django.conf import settings

from storages.backends.s3boto import S3BotoStorage
from storages.backends.mongodb import GridFSStorage
from storages.backends.azure_storage import AzureStorage

STORAGES = settings.DISTRIBUTED_STORAGES


class DistributedStorage(Storage):
    def __init__(self):
        super(DistributedStorage, self).__init__()

        self.storages_dict = {}
        self.storages=[]
        if constants.S3_BOTO in STORAGES:
            try:
                self.s3_storage = S3BotoStorage()
                self.storages.append(self.s3_storage)
                self.storages_dict.update({
                        constants.S3_BOTO: self.s3_sotrage})
            except Exception, e:
                raise e
        else:
            self.s3_storage = None

        if constants.AZURE in STORAGES:
            try:
                self.azure_storage = AzureStorage()
                self.storages_dict.update({
                        constants.AZURE: self.mongo_storage})
                self.storages.append(self.azure_storage)
            except Exception, e:
                raise e
        else:
            self.azure_storage = None




        if constants.MONGO_DB in STORAGES:
            try:
                self.mongo_storage = GridFSStorage()
                self.storages_dict.update({
                        constants.MONGO_DB: self.mongo_storage})
                self.storages.append(self.mongo_storage)
            except Exception, e:
                raise e
        else:
            self.mongo_db = None
        print STORAGES
        #self.storages = [self.storages_dict[storage]
        #                 for storage in STORAGES]

    def _save(self, name, content):
        print 'inside save'
        print self.storages
        for storage in self.storages:
            print storage._save(name, content)

        return name

    def _open(self, name, mode='rb'):
        for storage in self.storages:
            if storage.exists(name):
                return storage._open(name, mode="rb")
        else:
            raise IOError('File %s doesn\'t exists' % name)

    def exists(self, name):
        exists = [storage.exists(name) for storage in self.storages]
        return any(exists)

    def delete(self, name):
        for storage in self.storages:
            storage.delete(name)

    def listdir(self, name):
        for storage in self.storages:
            storage.listdir(name)

    def size(self, name):
        size = 0
        for storage in self.storages:
            size = storage.size(name)

        return size

    def url(self, name):
        return self.storages[0].url(self, name)

