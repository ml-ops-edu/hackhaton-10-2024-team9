"""
Helper classes, provides default TestCase class set up and tear down.
"""

import logging
import unittest

class Consts:
    @staticmethod
    def range(*args):
        return ''.join([chr(i) for a in args for i in range(ord(a[0]), ord(a[1])+1)])

    @property
    def catalog(self):
        return self._catalog

    @property
    def unsetValue(self):
        return 'CHANGE.ME'

    @property
    def accents(self):
        return r"ÅÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÇåáàâäéèêëíìîïóòôöúùûüçõã"

    @property
    def symbols(self):
        return r"""~`!@#$%^&*()_-+={}[]\|:;"<>,./?"""

    @property
    def specials(self):
        return r"""`¡™£¢∞§¶•ªº–≠œ∑´®†\¨ˆøπ“‘«åß∂ƒ©˙∆˚¬…æΩ≈ç√∫˜µ≤≥÷`⁄€‹›ﬁﬂ‡°·‚—±Œ„´‰ˇÁ¨ˆØ∏”’»ÅÍÎÏ˝ÓÔÒÚÆ¸˛Ç◊ı˜Â¯˘¿"""

    @property
    def minTiny(self):
        return -2**7

    @property
    def maxTiny(self):
        return 2**7-1

    @property
    def minSmall(self):
        return -2**15

    @property
    def maxSmall(self):
        return -2**15

    @property
    def minBig(self):
        return -2**63

    @property
    def maxBig(self):
        return 2**63-1

    @property
    def minInt(self):
        return -2**31

    @property
    def maxInt(self):
        return 2**31-1

class classproperty:
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, owner):
        return self.fget(owner)

class CustomLogFormatter(logging.Formatter):
    def format(self, record):
        if hasattr(record, 'nl') and record.nl:
            return '\n'+super().format(record)
        else:
            return super().format(record)

class TrinoConnect(Consts):
    loggers = {}

    @classproperty
    def logger(cls):
        return cls.loggers[cls.__name__]

    @classmethod
    def setUpTrait(cls):
        import uuid
        import configparser
        import os
        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        config = configparser.ConfigParser()
        config.read('private/config.ini')
        targetEnv = os.getenv('TRINO_TEST_ENV', config.get('default', option='target', fallback='default'))
        targetTrino=config.get(targetEnv, option='trino', fallback='default')
        targetS3=config.get(targetEnv, option='s3', fallback='default')

        if cls.__name__ not in cls.loggers:
            logger = logging.getLogger(cls.__name__)
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(
                CustomLogFormatter('%(asctime)s %(name)s %(levelname)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            )
            logger.addHandler(handler)
            logger.propagete = False
            cls.loggers[cls.__name__] = logger

        cls.caS3=os.getenv('CA_BUNDLE', config.get(targetS3, option='ca', fallback="true"))
        cls.trinoHost=os.getenv('TRINO_HOST', config.get(targetTrino, option='host', fallback='localhost'))
        cls.trinoPort=os.getenv('TRINO_PORT', int(config.get(targetTrino, option='port', fallback=8080)))
        cls.trinoScheme=config.get(targetTrino, option='scheme', fallback='http')
        cls.schemaNm=f"unittest_{str(uuid.uuid4())[:8]}"
        cls.s3Bucket=os.getenv('AWS_S3_BUCKET', config.get(targetS3, option='bucket', fallback=None))
        cls.s3AccessKey=os.getenv('AWS_ACCESS_KEY_ID', config.get(targetS3, option='key', fallback=None))
        cls.s3SecretKey=os.getenv('AWS_SECRET_ACCESS_KEY', config.get(targetS3, option='secret', fallback=None))
        cls.s3Endpoint=os.getenv('AWS_S3_ENDPOINT', config.get(targetS3, option='endpoint', fallback=None))

        if cls.s3Bucket is None or cls.s3Bucket == cls.unsetValue:
            raise Exception("S3 bucket not set")

        cls.logger.info(f"Schema: {cls._catalog}.{cls.schemaNm}")
        cls.logger.info(f"S3 locations: s3a://{cls.s3Bucket}/trino/data/unittest/{cls.schemaNm}")
        if cls.caS3.lower() == 'true':
            cls.caS3 = True
        elif cls.caS3.lower() == 'false':
            cls.caS3 = False
        cls.logger.info(f"CA: {cls.caS3}")

        cls.conn = connect(
            host=cls.trinoHost,
            port=cls.trinoPort,
            user='test',
            http_scheme=cls.trinoScheme,
            verify=False
        )

    @classmethod
    def deleteS3Folder(cls, s3, path):
        if len(path) < 2:
            raise RuntimeError(f'Invalid path: {path}')
        if not path.endswith('/'):
            path += '/'

        if s3.__class__.__module__ == 'botocore.client':
            # <Boto3>
            cls.logger.info(f'Boto3 {cls.s3Bucket}/{path}')
            objects_to_delete = []
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=cls.s3Bucket, Prefix=path):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})
                    if objects_to_delete:
                        s3.delete_objects(
                            Bucket=cls.s3Bucket,
                            Delete={'Objects': objects_to_delete}
                        )
                        cls.logger.info(f'Deleted {len(objects_to_delete)} objects in {path}')
        elif s3.__class__.__module__ == 'pyarrow._s3fs':
            # <PyArrow>
            cls.logger.info(f'PyArrow {cls.s3Bucket}/{path}')
            # fileInfo = s3.get_file_info(f"{cls.s3Bucket}/trino/data/unittest/{cls.schemaNm}")
            # if fileInfo.type != fs.FileType.NotFound:
            #     cls.logger.warning(f"S3 location {cls.s3Bucket}/{path} not deleted")
            s3.delete_dir(f"{cls.s3Bucket}/trino/data/unittest/{cls.schemaNm}")
            s3.delete_dir(f"{cls.s3Bucket}/trino/warehouse/{cls.schemaNm}")
        else:
            raise RuntimeError(f'Unsupported S3 client: {type(s3)}')
            

    @classmethod
    def tearDownTrait(cls):
        from contextlib import closing

        if cls.conn != None:
            cls.logger.info(f"Drop schema: {cls._catalog}.{cls.schemaNm}", extra={'nl': True})
            with closing(cls.conn.cursor()) as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {cls._catalog}.{cls.schemaNm} CASCADE")
            cls.conn.close()
        if cls.schemaNm != None:
            if sum(o is None or o == cls.unsetValue for o in [ cls.s3AccessKey, cls.s3SecretKey, cls.s3Endpoint ]) == 0:
                try:
                    cls.logger.info(f"Delete s3a://{cls.s3Bucket}/trino/data/unittest/{cls.schemaNm}")

                    # <<<<< Boto3 >>>>>
                    import boto3
                    import warnings
                    warnings.filterwarnings(action='ignore', module='.*botocore.*', category=DeprecationWarning)
                    s3 = boto3.client('s3', aws_access_key_id=cls.s3AccessKey, aws_secret_access_key=cls.s3SecretKey, endpoint_url='https://'+cls.s3Endpoint, verify=cls.caS3)

                    # <<<<< PyArrow (slow!) >>>>>
                    # from pyarrow import fs
                    # s3 = fs.S3FileSystem(access_key=cls.s3AccessKey, secret_key=cls.s3SecretKey, endpoint_override=cls.s3Endpoint)

                    cls.deleteS3Folder(s3, f'trino/data/unittest/{cls.schemaNm}')
                    cls.deleteS3Folder(s3, f'trino/warehouse/{cls.schemaNm}')
                except FileNotFoundError:
                    pass
                except Exception as e:
                    cls.logger.warning(f'S3 Cleanup {e}')
            else:
                cls.logger.warning(f"Skip S3 cleanup at location s3a://{cls.s3Bucket}/trino/data/unittest/{cls.schemaNm}")
        cls.logger.info(f"Done")

class TestCase(unittest.TestCase, TrinoConnect):
    @classmethod
    def setUpClass(cls):
        cls.setUpTrait()

    @classmethod
    def tearDownClass(cls):
        cls.tearDownTrait()

