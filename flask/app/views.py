# jsonify creates a json representation of the response

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.pylab import *  
from matplotlib.figure import Figure
import io
import base64
from flask import jsonify
from flask import render_template
from app import app
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import random
import StringIO
from flask import Flask, make_response, redirect, url_for, render_template, request
import seaborn as sns
import time  


auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra') # Setting up connections to cassandra
cluster = Cluster(['35.164.226.10', '52.24.193.4', '52.24.193.4', '54.245.7.62']) # provide the IPs for Cassandra cluster
session = cluster.connect()
session.execute('USE ' + "Energy_consumption") # provide keyspace name                                                       



@app.route('/')                                                                 
def index():                                                                    
    return render_template('index.html')  # define index page     


@app.route('/api/<ID>') # get historical data for a certain ID

def get_data(ID):
       stmt = "SELECT * FROM rawdata WHERE State='AK' and ID=%s "
       response = session.execute(stmt, parameters=[ID])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"ID": x.id, "State": x.state, "Business": x.id, "Scale": x.scale, "Electricity": x.efacility} for x in response_list]
       return jsonify(emails=jsonresponse)

@app.route('/api/<State>')

def get_data(State): # get historical data for a state
       stmt = "SELECT * FROM rawdata WHERE State=%s "
       response = session.execute(stmt, parameters=[State])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"ID": x.id, "State": x.state, "Business": x.id, "Scale": x.scale, "Electricity": x.efacility} for x in response_list]
       return jsonify(emails=jsonresponse)

@app.route('/queries', methods=['GET', 'POST']) # redirect the page when receiving queries of State and Business type
def home():
    if request.method == 'POST':
         a = request.form['a']
         b = request.form['b']
         A=['http://ec2-52-13-146-195.us-west-2.compute.amazonaws.com/',a,'/',b]

         URL=''.join(A)
         return redirect(URL)



@app.route('/<State>/<Business>', methods=['GET', 'POST']) # define the route of State/Business

def draw(State, Business):

    img = io.BytesIO()
    
    sns.set_style("whitegrid")
   
    stmt = "SELECT * FROM recentdata WHERE State=%s and Business=%s "
    response = session.execute(stmt, parameters=[State, Business])  # read data from cassandra table
    response_list = []
    for val in response:
            response_list.append(val)
    jsonresponse = [{"ID": x.id, "Efacility": x.efacility,"Ecoll": x.ecoll, "Efan": x.efan, "Gfacility": x.gfacility, "Eheat": x.eheat, "Gheat": x.gheat, "Einterequip": x.einterequip, "Ginterequip": x.ginterequip, "Einterlight": x.einterlight, "Gwater":x.gwater} for x in response_list]
    
    d=pd.DataFrame(jsonresponse) # put the data into a dataframe "d"
    y1=d.loc[:,'Gwater'].tolist() # get list for each queried parameter
    y2=d.loc[:,'Ecoll'].tolist()
    y3=d.loc[:,'Efan'].tolist()
    y4=d.loc[:,'Gfacility'].tolist()
    y5=d.loc[:,'Efacility'].tolist()
    y6=d.loc[:,'Gheat'].tolist()
    y7=d.loc[:,'Einterequip'].tolist()
    y8=d.loc[:,'Ginterequip'].tolist()
    y9=d.loc[:,'Einterlight'].tolist()
    
    y1 = [item for item in y1 if item >= 0] # only keep positive values
    y2 = [item for item in y2 if item >= 0]
    y3 = [item for item in y3 if item >= 0]
    y4 = [item for item in y4 if item >= 0]
    # creat subplots, here only 4 parameters are selected, but you can select any parameter you want
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(0.1,2))
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(12, 6)

    ax1.title.set_text('Gas-heating')
    ax2.title.set_text('Electricity-cooling')
    ax3.title.set_text('Electricity-lighting')
    ax4.title.set_text('Electricity-equipment')

    # creat histogram

    ax1.hist(y6)
    ax2.hist(y2)
    ax3.hist(y9)
    ax4.hist(y7)

    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()

    # get the list larger than certain percentile
    df2=pd.DataFrame(jsonresponse)
    df2.set_index('ID', inplace=True)
    col_names =  ['Category', 'list']
    Reclist  = pd.DataFrame(columns = col_names)
    Reclist['Category']=['Gheat', 'Ecoll','Einterlight', 'Einterequip']

    for i in range (0,4):
       P=df2[df2.iloc[:,i] > df2.iloc[:,i].quantile(0.9)].index.tolist()
       Reclist.at[i, 'list']=P

    return render_template('plot.html',tables=[Reclist.to_html(classes='female')], titles = ['Alert List'], plot_url= plot_url)


   

if __name__ == '__main__':                                                      
    socketio.run(app, debug=True) 


  





