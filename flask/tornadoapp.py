from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from app import app
from cassandra.auth import PlainTextAuthProvider

class MainHandler(RequestHandler):
 def get(self):
   self.write("This message comes from Tornado ^_^")

tr = WSGIContainer(app)

application = Application([
(r"/tornado", MainHandler),
(r".*", FallbackHandler, dict(fallback=tr)),
])

if __name__ == "__main__":
 CASSANDRA_SETUP_KWARGS = {'protocol_version': 3, "auth_provider": PlainTextAuthProvider(username='cassandra', password='cassandra')}
 application.listen(80)
 IOLoop.instance().start()
