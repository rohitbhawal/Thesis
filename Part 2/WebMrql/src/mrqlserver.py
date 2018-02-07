'''
Created on Jan 28, 2017

@author: Rohit Bhawal

References : 1. http://stackoverflow.com/questions/3431825/generating-a-md5-checksum-of-a-file
             2. http://python-cloudant.readthedocs.org/en/latest/getting_started.html#connecting-with-a-client
             3. http://flask.pocoo.org/docs/0.10/quickstart/
             4. https://pythonprogramming.net/
'''
import hashlib
import os
from flask import Flask, render_template, request, url_for, redirect, flash, session
from flask import make_response, current_app
from datetime import timedelta
from flask_session import Session
from functools import wraps, update_wrapper
import subprocess
import tempfile
from werkzeug import secure_filename
import shutil
from shutil import copyfile
import json
from threading import Thread
import ast  

sess = Session()
app = Flask(__name__)
app.secret_key = 'rohitbhawalskynet'

Uname = ''

MRQL_EXEC_DIR = '/home/rohitbhawal/Workspace/apache-mrql-0.9.6-incubating/bin/' #Dev Env
# MRQL_EXEC_DIR = '/home/ubuntu/apache-mrql-0.9.6-incubating/bin/' #Test Env
# MRQL_EXEC_DIR = '/export/home/rohitbhawal/apache-mrql-0.9.6/bin/'  #Prod Env

HADOOP_EXEC_DIR = ['/usr/local/hadoop/bin/hadoop'] #Dev Env & Test Env
# HADOOP_EXEC_DIR = ['/share/apps/hadoop/bin/hadoop', '--config', '/share/apps/conf-yarn'] #Prod Env

# HADOOP_CLUSTER_LINK = 'http://rohitbhawal.com:8088/cluster' #Test Env
HADOOP_CLUSTER_LINK = 'http://hadoop.uta.edu:8088/cluster' #Prod Env

HOME_DIR = os.path.dirname(__file__)
WORK_DIR = os.path.join(tempfile.gettempdir(),'WebMRQL')
HDFS_DIR = '/user/rohitbhawal'
DATAFILELIST = 'userDataFilesList.txt'
LOGINFILE = 'logins.txt'
DATAFOLDER = 'data'
QUERYFOLDER = 'queries'
QUERYFILE = 'query.mrql'
QUERYFILE_DIST = 'query_dist.mrql'
THREADINFOFILE = 'thread_info.mrqlweb'
RESULTJSON = 'result.json'
JSONKEYSEP = "_"
NETID_LOGIN = True
SESSION_ALIVE_TIMEOUT_MINUTES = 60
# SUB_URL = '/mrql'    #Test Env & Prod Env
SUB_URL = ''    #Dev Env


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()
 
    def get_methods():
        if methods is not None:
            return methods
 
        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']
 
    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp
            h = resp.headers
 
            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
                
            return resp
 
        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def login_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if 'logged_in' in session:
            return f(*args, **kwargs)
        else:
            error = "You need to login first"
            return redirect(url_for('loginPage', error=error))
    return wrap

@app.route('/')
def homepage():
    return render_template("main.html", subURL = SUB_URL)

@app.route('/login/', methods=["GET","POST"])
def loginPage():
    error = ''
    if 'error' in request.args:
        error = request.args['error']
    try:
        if request.method == "POST":
            uname = request.form['username']
            upwd = request.form['password']
            valid = checkLogin(uname, upwd)
            if valid:
                Uname = uname.upper()
                session['logged_in'] = True
                session['username'] = Uname
                return redirect(url_for('upload', Uname=Uname))
            else:
                error = 'Invalid login credentials ! Try Again...'
        
        #return render_template("login.html", error = error)
    except Exception as e:
        flash(e)    
    return render_template("login.html", error = error, subURL = SUB_URL) 

def checkLogin(uname, upwd):
    allowed = False
    loginFile = os.path.join(HOME_DIR, LOGINFILE)
    with open(loginFile, 'r') as login:
        for data in login:
            value = data.split(',')
            if uname.lower() == value[0].strip().lower():
                if NETID_LOGIN and not value[1].strip():
                    allowed = netID_login(uname, upwd) 
                else:
                    if upwd.strip() == value[1].strip():
                        allowed = True
                break

    return allowed

def netID_login(uname, upwd):
    allowed = False   
    
    args = ['kinit', uname]
    
    output = executeShell(args, upwd);
    
    if not output['Error']:
        allowed = True
    
    return allowed
    
@app.route('/logout/')
@login_required
def logout():  
#     removeUserData(session['username'].lower())
    session.clear()
    error = "You have Successfully Logged Out !"
    return redirect(url_for('loginPage', error = error)) 
          
@app.route('/graph/', methods=['GET', 'POST'])
@login_required
def graph():
    Uname = request.args['Uname']
    xaxis = str(request.args['xaxis']).strip()
    yaxis = str(request.args['yaxis']).strip()
    jsonKeys = processJSON(Uname.lower())
    result = getResult(Uname.lower())
    
    return render_template("graph.html", Uname=Uname, xaxis = xaxis, yaxis=yaxis, jsonKeys = jsonKeys, result=result, subURL = SUB_URL)

def getResult(uname):
    wrkDir = getWrkDir(uname)
    resultjsonpath = os.path.join(wrkDir, RESULTJSON)
    result = ''
    if os.path.exists(resultjsonpath):
        jsonoutput = open(resultjsonpath, 'r').readlines()
        result = jsonoutput[0]
    return result   

@app.route('/graphdata/', methods=['GET', 'POST'])
@login_required
@crossdomain(origin='*')
def graphdata():
    uname = session['username']
    xaxis = request.args['xaxis']
    yaxis = request.args['yaxis']
    
    result = getData(uname.lower(), xaxis, yaxis)
    
    return result
    
def getData(uname, xaxis, yaxis):
    wrkDir = getWrkDir(uname)
    resultjsonpath = os.path.join(wrkDir, RESULTJSON)
    resultdata = []
    if os.path.exists(resultjsonpath):
        jsonoutput = open(resultjsonpath, 'r').readlines()
        jsonoutput = jsonoutput[0]
        try:
            jsondata = json.loads(jsonoutput)
            xaxis_data = parseJSONData(jsondata, xaxis)
            yaxis_data = parseJSONData(jsondata, yaxis)
            
            for i in range(max(len(xaxis_data), len(yaxis_data))):
                resultdata.append(str(xaxis_data[i])+','+str(yaxis_data[i]))
        except Exception as e:
            print 'Error in Graph GetData:' +e
            return ''
    head = 'xAxis,yAxis\n'
    resultdata = head + '\n'.join(resultdata)
    return resultdata

def parseJSONData(jsonData, attr):
    data = []
    if jsonData.__class__.__name__ in ('list', 'tuple'):
        for i in range(len(jsonData)):
            data = data + parseJSONData(jsonData[i], attr)
    else:
        try:
            attrList = attr.split(JSONKEYSEP)
            jkey = attrList[0]
            attrList.remove(jkey)
            attr = '_'.join(attrList)
            if attr == '':
                data.append(jsonData[jkey])
            else:
                data = parseJSONData(jsonData[jkey], attr)
        except Exception as e:
            print 'Error in parseJSONData:' +e
            return ''   
    return data

@app.route('/checkThread/', methods=["GET", "POST"])
@login_required
def checkThread():
    
    uname = request.args['Uname']
    
    threadInfo = getThreadInfo(uname.lower())
    if threadInfo:
        if threadInfo['status'] == 'finished':
            result = "thread_finished"
        else:
            result = "thread_working"
    else:
        result = "no_thread"
    
    return result
    
@app.route('/upload/', methods=["GET", "POST"])
@login_required
def upload(): 
    result = ''
    query = ''
    runType= ''
    runMode = ''
    optMode = ''
    nodes = ''
    jsonKeys = []
    
    Uname = request.args['Uname'] 
    queryList = buildSelectQuery()
    dataList = buildPredefineDataFileList()
    selectQuery = request.args.get('selectQuery', 'Select Query')
    wrkDir = getWrkDir(Uname.lower())
    queryFile = os.path.join(wrkDir, QUERYFILE)
    resultjsonpath = os.path.join(wrkDir, RESULTJSON)
    threadInfoFile = os.path.join(wrkDir, THREADINFOFILE)
    
    try:
        if request.method == "POST":  
            if 'visualize' in request.form:
                xaxis = request.form['x_axis']
                yaxis = request.form['y_axis']
                return redirect(url_for('graph',Uname=Uname, xaxis = xaxis, yaxis = yaxis))
                        
            if 'loadPrevSession' in request.form:
                if checkThreadFinished(Uname.lower()):#not thread.isAlive():
                    params = getThreadInfo(Uname.lower(), True)
                    if params:
                        runType = params['runType']
                        runMode = params['runMode']
                        nodes = params['runType']
                        nodes = params['nodes']  
                    
                    if os.path.exists(queryFile):
                        query = open(queryFile, 'r').read()
                    else:
                        query = "No Previous Query Found !"     
                    
                    if os.path.exists(resultjsonpath):
                        result = open(resultjsonpath, 'r').read()
                        jsonKeys = processJSON(Uname.lower())
                    else:
                        result = ""
                    
                    if os.path.exists(threadInfoFile):    
                        os.remove(threadInfoFile)                   
                    
                else:
                    query = request.form['query']
                    runType = request.form['run_type']
                    runMode = request.form['run_mode']
                    if 'nodes' in request.form:
                        nodes = request.form['nodes']
                    optMode = request.form['opt_mode']
                    result = "Processing Previous Query !!!"
            else:
                query = request.form['query']
                runType = request.form['run_type']
                runMode = request.form['run_mode']
                if 'nodes' in request.form:
                    nodes = request.form['nodes']
                optMode = request.form['opt_mode']
                
                if query:
                    fileList = request.files.getlist("dataFile[]")
                    if fileList[0]:
                        wrkDir = getWrkDir(Uname.lower())
                        saveFileList(wrkDir, fileList)
                    dataChoiceList = request.form.getlist("dataChoice[]")
                    UserDataChoiceList = request.form.getlist("UserDataChoice[]")
    #                 if runMode.lower().strip() == 'distributed':
                    if checkThreadFinished(Uname.lower()): #not thread.isAlive():
                        if runMode.lower().strip() == 'distributed':
                            if not fileList[0] and not dataChoiceList and not UserDataChoiceList:
                                result = "Choose/Upload Data Files Again to use Distributed Mode"
                            else:
                                processThreadForDistMode(query, Uname.lower(), runType, runMode, optMode, 
                                                     nodes, fileList, dataChoiceList, UserDataChoiceList)
                                result = "Query Submitted !!!\nYou can check progress in \n\n" + HADOOP_CLUSTER_LINK
                        else:
                            result = processQuery(query, Uname.lower(), runType, runMode, optMode, 
                                                  nodes, fileList, dataChoiceList, UserDataChoiceList)
                            jsonKeys = processJSON(Uname.lower())
                    else:
                        runMode = 'distributed'
                        result = "Processing Previous Query !!!"
                
                else:
                    result = 'Enter Query !'
        else:
            if(selectQuery in 'Select Query'):
                query = ""
            else:
                query = getSelectQuery(selectQuery)

            threadInfo = getThreadInfo(Uname.lower())
            if threadInfo:
                params = threadInfo['params']
                if params:
                        runType = params['runType']
                        runMode = params['runMode']
                        nodes = params['runType']
                        nodes = params['nodes']
                        
                if os.path.exists(queryFile):
                        query = open(queryFile, 'r').read() 
                        query = "/** Loading Previous Query **/\n\n" + query
                if threadInfo['status'] == 'finished':                  
                    if os.path.exists(resultjsonpath):
                        result = open(resultjsonpath, 'r').read()
                        jsonKeys = processJSON(Uname.lower())
                    os.remove(threadInfoFile)
                else:
                    result = "Processing Previous Query !!!"      

    except Exception as e:
        print e
        os.chdir(HOME_DIR)
        result = e
    
    userDataList = buildUserDataFileList(Uname.lower())
    
    return render_template("upload.html", Uname=Uname, result=result, query=query, selectQuery=selectQuery, queryList=queryList, \
                               dataList = dataList,runT=runType, runM=runMode, optM=optMode, nodes=nodes, userdataList = userDataList, \
                               jsonKeys = jsonKeys, subURL = SUB_URL)

def processQuery(query, uname, runType, runMode, optMode, nodes, fileList, dataChoiceList, UserDataChoiceList, threadMode = False):
    global thread_finished
    distMode = False
    holdQuery = query
    param = ''
    if threadMode:
        param = {'runType': runType, 'runMode': runMode,'optMode': optMode, 'nodes': nodes}
        updateThreadInfo(uname, 'started', str(param))
    
    runArg = chooseRunType(runType, runMode, optMode, nodes)
 
    if '-dist' in runArg:
        distMode = True
 
    wrkDir = getWrkDir(uname)
    
    query = addDumpJson(query, wrkDir)
    
    mrqlFileDir = os.path.join(wrkDir, QUERYFILE)
    mrqlFileDir_DIST = os.path.join(wrkDir, QUERYFILE_DIST)
    resultjsonpath = os.path.join(wrkDir, RESULTJSON)
    
    if distMode:
        #Clear all files from HDFS for the user
        removeFileFromHDFS(os.path.join(wrkDir, "*"))
    
    
    with open(mrqlFileDir, 'w') as qFile:
        qFile.write(query)
        qFile.close() 
        
    if distMode:
        with open(mrqlFileDir_DIST, 'w') as qFile:
            qFile.write(query)
            qFile.close() 
    
#     fileList = request.files.getlist("dataFile[]")
    if fileList[0]:
#         saveFileList(wrkDir, fileList)
        if distMode:
            makeProperQueryFileForDistMode(uname, mrqlFileDir, mrqlFileDir_DIST, fileList)
            
#     dataChoiceList = request.form.getlist("dataChoice[]")
#     UserDataChoiceList = request.form.getlist("UserDataChoice[]")
    
    if dataChoiceList:
        copySampleQueryData(dataChoiceList, wrkDir)
        saveFileList(wrkDir, dataChoiceList, False)
        if distMode:
            makeProperQueryFileForDistMode(uname, mrqlFileDir, mrqlFileDir_DIST, dataChoiceList)
            
    if UserDataChoiceList and distMode:
        makeProperQueryFileForDistMode(uname, mrqlFileDir, mrqlFileDir_DIST, UserDataChoiceList)
        
    os.chdir(wrkDir)
    
    if distMode:
        if os.path.exists(mrqlFileDir_DIST):
            runArg.append(str(mrqlFileDir_DIST))
        else:
            raise Exception('Choose Data Files for Distributed Mode Query Processing !')
    else:
        runArg.append(str(mrqlFileDir))
    
    
    
    if os.path.exists(resultjsonpath):
        os.remove(resultjsonpath)
    
    
    if threadMode:
        updateThreadInfo(uname, 'running', str(param))
        
    #Sending Query to MRQL   
    output = executeShell(runArg)
    
    if distMode:
        moveFileFromHDFS(resultjsonpath)
    
    #If resultjson file got created that means no error occured while processing the query
    if os.path.exists(resultjsonpath):
        jsonoutput = open(resultjsonpath, 'r').read()
#         jsonoutput = jsonoutput[0]
    else:
        if output['Error']:
            jsonoutput = output['Error']  #output + '\n' + 
            print 'Error in MRQL Query Processing: ' + output['Error']
            if threadMode:
                with open(resultjsonpath, 'w') as qFile:
                    qFile.write(jsonoutput)
                    qFile.close()
            
        else:
            jsonoutput = output['Output']
            
    if '-trace' in runArg or '-info' in runArg:
        jsonoutput = output['Output'] + '\nResults:\n' + jsonoutput
            
    with open(mrqlFileDir, 'w') as qFile:
        qFile.write(holdQuery)
        qFile.close()
           
    os.chdir(HOME_DIR)
#     removeUserData(wrkDir)

    if threadMode:
        updateThreadInfo(uname, 'finished', str(param))
        #thread_finished = True       
        
    return jsonoutput

def processJSON(uname):
    jsonKeys = ''
    wrkDir = getWrkDir(uname)
    resultjsonpath = os.path.join(wrkDir, RESULTJSON)
    if os.path.exists(resultjsonpath):
        jsonoutput = open(resultjsonpath, 'r').read()
#         jsonoutput = jsonoutput[0]
        
        try:
            jsondata = json.loads(jsonoutput)
            jsonKeys = getJsonKeys(jsondata)
            jsonKeys = jsonKeys.keys()   
            if jsonKeys[0] == '':
                jsonKeys = ''     
        except Exception as e:
            jsonKeys = ''
        
    return jsonKeys

def getJsonKeys(jsonData, level = -1, parentKey = ''):
    jsonKeys = dict()
    if jsonData.__class__.__name__ in ('list', 'tuple'):
        if (len(jsonData) > 0):
            jsonKeys.update(getJsonKeys(jsonData[0], level, parentKey))
    else:
        try:
            jsonKeysList = jsonData.keys()
            level += 1
            for jKey in jsonKeysList:
                nKey = jKey
                if parentKey:
                    nKey = parentKey + JSONKEYSEP + jKey
               
                jsonKeys.update(getJsonKeys(jsonData[jKey], level, nKey))
        except Exception as e:
            jsonKeys[parentKey] = level
            
    return jsonKeys

def chooseRunType(runType, runMode, optMode, nodes):
    runArg = []
    distMode = False
    if runType.lower().strip() == 'mapreduce':
        arg = "mrql"
    else:
        if runType.lower().strip() == 'spark':
            arg = "mrql.spark" 
        else:
            arg = ""
    if arg:
        arg = MRQL_EXEC_DIR + arg
    runArg.append(arg)
    if runMode.lower().strip() == 'memory':
        arg = ""
    else: 
        if runMode.lower().strip() == 'local':
            arg = "-local"
        else:
            if runMode.lower().strip() == 'distributed':
                arg = "-dist"
                distMode = True
            else:
                arg = ""
    runArg.append(arg)
    if distMode:
        arg = "-nodes " + str(nodes)
        runArg.append(arg)
    
    if optMode.lower().strip() == 'trace':
        arg = "-trace"
        runArg.append(arg)
    else: 
        if optMode.lower().strip() == 'info':
            arg = "-info"
            runArg.append(arg)
    
    
    return runArg

def getWrkDir(uname):
        
    reqDir = os.path.join(WORK_DIR, uname)
    
    if not os.path.exists(reqDir):
        os.makedirs(reqDir)
#     os.chmod(reqDir, 777)
    return reqDir

#Save DataFile File in Working directory and update user Data File List
def saveFileList(wrkDir, fileList, saveData = True): 
    userFileExist = False
    readList = []
    filepath = os.path.join(wrkDir, DATAFILELIST)
    if os.path.exists(filepath):
        userDataFilesList = open(filepath, 'r')
        for fname in userDataFilesList.readlines():
            readList.append(fname.rstrip())
        userDataFilesList.close()
    userDataFilesList = open(filepath, 'a')
    
    for file in fileList:
        if file:
            if saveData:  
                filename = secure_filename(file.filename)
                file.save(os.path.join(wrkDir, filename))
            else:
                filename = file
            userFileExist = True
            if not filename in readList:
                userDataFilesList.write(filename)
                userDataFilesList.write('\n')
    userDataFilesList.close()
    return userFileExist

def copySampleQueryData(dataChoiceList, wrkDir):
    dataLoc = os.path.join(HOME_DIR, DATAFOLDER)
#     dataList = dataChoiceList
    for dataFile in dataChoiceList: 
        src = os.path.join(dataLoc, dataFile)
        dst = os.path.join(wrkDir, dataFile)
        
        #If file chosen is not from User Uploaded Data Files then Copy Again
#         if not uname.upper() in dataFile:
        copyfile(src, dst)
#         else:
        #If file chosen is from User Uploaded Data Files then rename filename by removing User Name from it
#             dataChoiceList.remove(dataFile)
#             dataFile = dataFile.replace(uname.upper(), "")
#             dataChoiceList.append(dataFile.strip())
            
  
def addDumpJson(query, path):
    result = query.split(";")
    resultdumppath = os.path.join(path, RESULTJSON)
    
    pos = len(result) - 2
    result[pos] = '\ndumpjson "' + resultdumppath + '" from ' + result[pos]
    
    return '; '.join(result)
    
def getSelectQuery(queryName):
    query = ''
    qpath = os.path.join(HOME_DIR, QUERYFOLDER)
    queryName = queryName + '.mrql'
    qpath = os.path.join(qpath, queryName)
    with open(qpath, 'r') as QFile:
        query = QFile.read()
    
    return query

def buildSelectQuery():
    queryList = []
    qpath = os.path.join(HOME_DIR, QUERYFOLDER)
     
    dirs = os.listdir( qpath )
    for file in dirs:
        if '~' not in file:
            if '.mrql' in file:
                file = file.replace('.mrql', '')
                queryList.append(file)
    
    return queryList

def buildPredefineDataFileList():
    dataList = []
    qpath = os.path.join(HOME_DIR, DATAFOLDER)
     
    dirs = os.listdir( qpath )
    for file in dirs:
        dataList.append(file)
    
    return dataList   

def buildUserDataFileList(uname):  
    wrkDir = getWrkDir(uname)
    filepath = os.path.join(wrkDir, DATAFILELIST)
    dataList = []
    filelist = ''
    if os.path.exists(filepath):
        filelist = open(filepath, 'r')
        for file in filelist.readlines():
            dataList.append(file.rstrip())
    
    return dataList 
        

def removeUserData(uname):
    WORK_DIR = tempfile.gettempdir()
    reqDir = os.path.join(WORK_DIR, uname)
    if os.path.exists(reqDir):
        shutil.rmtree(reqDir)

def getHash(buf):
    hasher = hashlib.md5()
    hasher.update(buf)
    return hasher.hexdigest()

def executeShell(runArg, data = ''):
    proc = subprocess.Popen(runArg, 
                        stdin=subprocess.PIPE, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
    
    if data:
        proc.stdin.write(data) 
    proc.stdin.flush() 
    output, stderr = proc.communicate()
    
    result = {'Output': output, 'Error': stderr}
    
    return result

def makeProperQueryFileForDistMode(uname, queryFileLoc, distQueryFileLoc, fileList):
    wrkDir = getWrkDir(uname)
    tmpQuery = str(open(queryFileLoc, 'r').read())
    for file in fileList:
        try:
            tmpQuery = tmpQuery.replace(file.filename, wrkDir + '/' + file.filename)
            moveFileToHDFS(uname, file.filename)
        except Exception as e:
            tmpQuery = tmpQuery.replace(file, wrkDir + '/' + file)
            moveFileToHDFS(uname, file)
    
    with open(distQueryFileLoc, 'w') as qFile:
        qFile.write(tmpQuery)
        qFile.close() 

def moveFileToHDFS(uname, filename):
    wrkDir = getWrkDir(uname)
    filename = os.path.join(wrkDir, filename)
    
    
#     hdfs_dir = str(HDFS_DIR + '/' + uname)
    
    args = HADOOP_EXEC_DIR + ['fs', '-mkdir', '-p', wrkDir]
    
    try:
        output = executeShell(args)
        
        args = HADOOP_EXEC_DIR + ['fs', '-put', filename, wrkDir]
        output = executeShell(args)
#         if not output['Output']:
#             raise Exception('Error in HDFS File Transfer:' + output['Output'])
#         else:
#             raise Exception('Error in HDFS DIR Creation:' + output['Output'])
    except Exception as e:
        print str(e)

def checkThreadFinished(uname):
    result = False
    threadInfo = getThreadInfo(uname)
    if threadInfo:
        if threadInfo['status'] == 'finished':
            result = True
    else:
        result = True

    return result
 
def updateThreadInfo(uname, status, param = ''):
    wrkDir = getWrkDir(uname)
    threadInfoFile = os.path.join(wrkDir, THREADINFOFILE)
    with open(threadInfoFile, 'w') as tFile:
        tdata = {'status': status, 'params' : param}
        tFile.write(str(tdata))
        tFile.close()

def getThreadInfo(uname, paramsOnly = False):
    wrkDir = getWrkDir(uname)
    threadInfoFile = os.path.join(wrkDir, THREADINFOFILE)
    if os.path.exists(threadInfoFile):
        tdata = open(threadInfoFile, 'r').read()
        tdata = ast.literal_eval(tdata)
        result = ast.literal_eval(tdata['params'])
        if not paramsOnly:
            tdata['params'] = result
            result = tdata        
    else:
        result = ''
    return result
  
def moveFileFromHDFS(path):         
    args = HADOOP_EXEC_DIR + ['fs', '-get', path]
    output = executeShell(args)

def removeFileFromHDFS(path):
    args = HADOOP_EXEC_DIR + ['fs', '-rm', path]
    output = executeShell(args)
    
def processThreadForDistMode(query, uname, runType, runMode, optMode, nodes, fileList, dataChoiceList, UserDataChoiceList):
    
    #thread_finished = False
    thread = Thread(target = processQuery, args = (query, uname, runType, runMode, optMode, nodes, 
                                                   fileList, dataChoiceList, UserDataChoiceList, True))
    thread.start()
    
@app.errorhandler(404)
def pageNotFound(e):
    return render_template("404.html")

@app.errorhandler(405)
def methodNotFound(e):
    return render_template("404.html")

@app.before_request
def make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=SESSION_ALIVE_TIMEOUT_MINUTES)


port = os.getenv('VCAP_APP_PORT','80')
if __name__ == '__main__':
    
    app.config['SESSION_TYPE'] = 'filesystem'
    
    sess.init_app(app)
    
    app.run(debug=True) #Dev Env

#     app.run(host='0.0.0.0/mrql', port=int(port),debug=True)     #Test Env & Prod Env
