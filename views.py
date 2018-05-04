# jsonify creates a json representation of the response

from flask import jsonify
from flask import render_template
from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['52.13.146.195', '54.71.37.147', '52.40.168.83', '54.245.27.74'])
session = cluster.connect()
session.execute('USE ' + "Energy_consumption")





# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
#session = cluster.connect("Energy_consumption")

@app.route('/')
@app.route('/index')
def index():
  return "insightdata project"

@app.route('/api/<State>')

def get_email(State):
       stmt = "SELECT * FROM rawdata WHERE State=%s "
       response = session.execute(stmt, parameters=[State])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"ID": x.id, "State": x.state, "Business": x.id, "Scale": x.scale, "Electricity": x.efacility} for x in response_list]
       return jsonify(emails=jsonresponse)