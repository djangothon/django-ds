# storage and fetch logic goes here

import constants

from django.core.files.storage import Storage
from django.conf import settings

from storages.backends.s3boto import S3BotoStorage
from storages.backends.mongodb import GridFSStorage


STORAGES = settings.DISTRIBUTED_STORAGES


class DistributedStorage(Storage):
    def __init__(self):
        super(DistributedStorage, self).__init__()

        if constants.MONGO_DB in STORAGES:
            try:
                self.s3_storage = S3BotoStorage()
            except Exception, e:
                raise e
        else:
            self.s3_storage = None

        if constants.MONGO_DB in STORAGES:
            try:
                self.mongo_db = GridFSStorage()
            except Exception, e:
                raise e
        else:
            self.mongo_db = None

    def _save(self, name, content):
        if self.s3_storage:
            self.s3_storage._save(name, content)

        if self.mongo_db:
            self.mongo_db._save(name, content)

        return name

    def exists(self, name):
        if self.s3_storage:
            exists = self.s3_storage.exists(name)
            if exists:
                return True

        if self.mongo_db:
            exists = self.mongo_db.exists(name)
            if exists:
                return True

        return False

    def delete(self, name):
        if self.s3_storage:
            self.s3_storage.delete(name)
        if self.mongo_db:
            self.mongo_db.delete(name)

    def listdir(self, name):
        if self.s3_storage:
            return self.s3_storage.listdir(name)
        if self.mongo_db:
            return self.mongo_db.listdir(name)

    def size(self, name):
        if self.s3_storage:
            return self.s3_storage.size(name)
        if self.mongo_db:
            return self.mongo_db.size(name)

    def url(self):
        pass
