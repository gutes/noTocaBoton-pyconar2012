import tornado.ioloop
import tornado.web
import logging
import sns
import sys

instance = None

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.application.SNSLogger.log("[WEB-instance-%s]: %s" % (instance, self.request), logging.INFO)
        self.write("<h1>Hola pyconar! yo soy la instancia <b>%s</b>!</h1>" % instance)

application = tornado.web.Application([
    (r"/", MainHandler),],
)

if __name__ == "__main__":
    instance = sys.argv[1]
    application.listen(80)
    application.SNSLogger = sns.SNSLogger("PyconAr2012")
    tornado.ioloop.IOLoop.instance().start()