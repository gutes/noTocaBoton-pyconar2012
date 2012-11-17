from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
from datetime import datetime
import types

class S3(object):
    def __init__(self, aws_access_key_id = None, aws_secret_access_key = None, **connection_options):

        if aws_access_key_id != None and aws_secret_access_key != None:
            connection_options["aws_access_key_id"] = aws_access_key_id
            connection_options["aws_secret_access_key"] = aws_secret_access_key
            
        self.s3_conn = self.__setup_connection(connection_options)
        
    def __setup_connection(self, options):
        return S3Connection(**options)
            
    def __call__(self, bucketname):
        return S3Bucket(self.s3_conn, bucketname)
        
class S3BucketException(Exception): pass
class S3Bucket(object):
    def __init__(self, connection, bucket_name):
        self.connection = connection
        self.bucket = self.connection.get_bucket(bucket_name)
        
    def __enter__(self):
        return self
    def __exit__(self, *args): pass
            
    def entry(self, key, **kargs):
        headers = {}
        etag = kargs.get("etag", None)
        if etag: 
            headers["If-Match"] = etag
        newer = kargs.get("newer", None)
        if newer:
            headers["If-Modified-Since"] = newer.isoformat()
        try:
            return self.bucket.get_key(key, headers = headers)       
        except S3ResponseError, e:
            if e.status == 412: # precondition fail.
                return None
            raise S3BucketException(e)

    def ls(self, prefix = ""):
        # get an instance of the specified bucket
        for k in self.bucket.get_all_keys(prefix = prefix):
            yield k

    def __null_progress_callback(self, action, *args): pass

    def get(self, key, dst_fd = None, **kargs):
        entry = self.entry(key, **kargs)
        if entry:
            progress_callback = kargs.get("progress", self.__null_progress_callback)
            
            if dst_fd:
                entry.get_contents_to_file(dst_fd, cb = lambda x,y: progress_callback("DOWNLOAD", key, dst_fd, x, y) )
                return
            else:
                return entry.get_contents_as_string(cb = lambda x,y: progress_callback("GET", key, x, y ) )
        raise S3BucketException("GET:invalid-s3-key: %s/%s" % (self.bucket.name, key) )

    def put(self, key, content, **kargs):
        entry = self.entry(key, **kargs)
        if entry == None: # there's no key with that name
            entry = self.bucket.new_key( key )
        progress_callback = kargs.get("progress", self.__null_progress_callback)
        
        if type(content) == types.FileType:
            entry.set_contents_from_file(content, cb = lambda x,y: progress_callback("UPLOAD", key, content, x, y ) )
        else:
            entry.set_contents_from_string(content, cb = lambda x,y: progress_callback("PUT", key, x, y ) )
        
    def rm(self, key, **kargs):
        entry = self.entry(key, **kargs)
        if entry:
            entry.delete()
            return
        raise S3BucketException("RM:invalid-s3-key: %s/%s" % (self.bucket.name, key) )        