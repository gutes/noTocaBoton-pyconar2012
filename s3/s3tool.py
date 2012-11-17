import os
import sys
import hashlib
import argparse
import json
from glob import iglob
from dateutil.parser import parse as date_parse
from s3api import S3, S3BucketException

class SyncTextUI(object):
    
    def __init__(self, bucket_name, access_key_id = None, secret_access_key = None):
        self.bucket_name = bucket_name
        self.s3 = S3(access_key_id, secret_access_key)
        
        self.commands = {
            "ls": self.ls,
            "get": self.get,
            "put": self.put,
            "rm": self.rm
        }
        self.__action_pending = False

    def ls(self, prefix = ""):    
        with self.s3(self.bucket_name) as bucket:
            items = ( (date_parse(item.last_modified).strftime("%Y-%m-%d %H:%M"), item.etag[1:-1], item.size, item.name) for item in bucket.ls(prefix))                
            fmt = None
            for item in sorted(items, key = lambda x:x[2], reverse=True):
                if fmt == None:
                    fmt = "%%s %%s %%0%ss %%s" % len(str(item[2]))
                print fmt % item
                
    def get(self, items):
        with self.s3(self.bucket_name) as bucket:
            for item in items:
                try:
                    with open(item, "w") as _dst:
                        bucket.get(item, _dst, progress = self.__progress)
                        if self.__action_pending:
                            self.__action_complete("GET", item, _dst)
                except S3BucketException, e:
                    os.unlink(item)

    def put(self, items):
        with self.s3(self.bucket_name) as bucket:
            for item in items:
                with open(item, "r") as _src:
                    bucket.put(item, _src, progress = self.__progress)
                    if self.__action_pending:
                        self.__action_complete("PUT", item, _src)

    def rm(self, entry):
        with self.s3(self.bucket_name) as bucket:
            bucket.rm(entry)
        
    def dispatch(self, parsed_arguments):
        for command, callback in self.commands.items():
            # lookup command as method
            if command in parsed_arguments:
                callback( getattr(parsed_arguments,command) )

    def __action_complete(self, action, key, fd):        
        self.__action_pending = False
        self.__progress(action, key, fd, 1, 1)
        sys.stdout.write("\n")
        
    def __progress(self, action, key, fd, at, size):
        self.__action_pending = True
        bar_size = 50
        p = (at*100) / size
        s = ("=" * (p / 2)) + ">" +(" " * (bar_size - (p / 2)))
        if action == "UPLOAD":
            direction = "<-"
        elif action == "DOWNLOAD":
            direction = "->"
        if at == 0:
            sys.stdout.write("[%s] %s:%s %s %s\n" % (action, self.bucket_name, fd.name, direction, key) )
        sys.stdout.write("\r%s (%d%%)" % (s, p) )
        sys.stdout.flush()


parser = argparse.ArgumentParser()
parser.add_argument('bucket', help='S3 bucket to use')

actions = parser.add_mutually_exclusive_group( required = True )
actions.add_argument('--ls', '-l', nargs="?", metavar='PREFIX', default=argparse.SUPPRESS, help="s3-side list entries with prefix PREFIX")
actions.add_argument('--rm', '-r', metavar='ENTRY', default=argparse.SUPPRESS, help="s3-side remove entry") 
actions.add_argument('--dup', '-d', nargs=2,  metavar=('SRC','DST'), default=argparse.SUPPRESS, help="s3-side duplicate entries" ) 
actions.add_argument('--get', '-g', nargs="+", metavar='PREFIX', default=argparse.SUPPRESS, help="retrieve entries from S3")
actions.add_argument('--put', '-p', nargs="*", metavar='PREFIX', default=argparse.SUPPRESS, help="store file to S3") 

args = parser.parse_args()

s3_sync_text_ui = SyncTextUI(
    bucket_name = args.bucket
)
s3_sync_text_ui.dispatch(args)        






        