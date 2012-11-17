import credenciales
import boto
from boto import sns
import logging

class SNSLogger(logging.Logger):
    def __init__(self, topicName, level=logging.info):
        self.snsConn = sns.SNSConnection(credenciales.access_key_id, credenciales.secret_access_key)
        self.loggingTopicArn = None
        topics = self.snsConn.get_all_topics()["ListTopicsResponse"]["ListTopicsResult"]["Topics"]
        
        for t in topics:
            if t["TopicArn"].split(':')[5] == topicName:
                self.loggingTopicArn = t["TopicArn"]
                break
                
        logging.Logger.__init__(self, level)
        
    
    def log(self, lvl, msg, *args, **kwargs):
        if self.loggingTopicArn is not None:
            self.snsConn.publish(self.loggingTopicArn, "%s:%s" % (lvl, msg) )
    
    def debug(self, msg, *args, **kwargs):
        if self.loggingTopicArn is not None:
            self.snsConn.publish(self.loggingTopicArn, "%s:%s" % (logging.DEBUG, msg) )
        
        