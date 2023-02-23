import asyncio
import websockets
import time
import json
import pymysql
import queue
import html
import ctypes
from threading import Thread
import logging
import traceback
from uuid import uuid4
import redis
import os
from logging.handlers import TimedRotatingFileHandler

import re
websocket_users = set()
websocket_client=set()
async def check_user_permit(websocket):
    # return True
    # websocket_users.add(websocket)
    # t=0
    while True:
        try:
            await asyncio.sleep(0.5)
            await websocket.send('connected')
            recv_str =  await asyncio.wait_for(websocket.recv(),10)
            loginJson=json.loads(recv_str)
            usr=loginJson['usr']
            pwd=loginJson['pass']
            deviceID=loginJson['deviceID']
            deviceName=loginJson['deviceName']
            token='-2'
            if 'token' in loginJson :
                tokenArray=loginJson['token'].split('#')
                token=tokenArray[0]
                if token != '-1' and token !='-2' and tokenArray[1] != usr+deviceID :
                    loginRes=[False,-3]
                else:
                    loginRes=loginAccount(usr,pwd,deviceID,deviceName,token)
            else:
                loginRes=loginAccount(usr,pwd,deviceID,deviceName,token)
            logger.info(usr+'-'+pwd+'-'+deviceID+'-'+str(loginRes))
            if(loginRes[0]==True):
                #删除重复在线的先
                findSocket=False
                for i in websocket_users:
                    if(i[1]==usr and i[2]==deviceID):
                        tempSocket=i[0]
                        websocket_users.remove((tempSocket,usr,deviceID))
                        websocket_client.remove(tempSocket)
                        findSocket=True
                        break                
                websocket_users.add((websocket,usr,deviceID))
                websocket_client.add(websocket)
                msgQueue[websocket]=queue.Queue()
                sendData={"type":"success","expired":loginRes[1],"token":loginRes[2]}
                msgQueue[websocket].put(json.dumps(sendData))
                if(findSocket==True):
                        try:
                            await tempSocket.send('{"type":"exit"}')
                        except Exception as e:
                            pass
                        try:
                            await tempSocket.close()
                        except Exception as e:
                            pass
            else:
                await websocket.send('{"type":"failed","code":'+'"'+loginRes[1]+'"'+'}')
                if(loginRes[1]=='-3'):
                    h5Client=getDevice(usr,'-1')
                    if(h5Client!='-1'):
                        msgQueue[h5Client].put('{"type":"newDevice","deviceID":'+'"'+deviceID+'"'+'}') 
                await websocket.close()
                raise Exception ('exit')
            return True
        except Exception as e :
            await websocket.close()
            raise Exception (str(e))
            return
async def recv_user_msg(websocket):
    while True:
        try:
            recv_text = await asyncio.wait_for(websocket.recv(),10 * 60 )
            jsonData=json.loads(recv_text)
            dataType=jsonData['type']
            if(dataType!='alive'):
                logger.info(jsonData)
                if(dataType=='task'):
                    target=getDevice(jsonData['usr'],jsonData['deviceID'])
                    if(target!='-1'):
                     
                        msgQueue[target].put(recv_text)
                    else:
                        cbClient=getDevice(jsonData['usr'],'-1')
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"taskUrl","taskId":jsonData['taskId'],"status":'-1',"msg":'设备掉线'}))
                elif(dataType=='getOnline'):
                    tempData=jsonData['deviceList']
                    usr=jsonData['usr']
                    sendData=[]
                    for j in tempData:
                        if getDevice(usr,j)!='-1':
                            sendData.append('1')
                        else:
                            sendData.append('0')
                                    
                    jsonData['deviceList']=sendData
                    msgQueue[websocket].put(json.dumps(jsonData)) 
                elif(dataType=='taskRes'):
                    initActivity(jsonData)
                elif(dataType=='updateDevice'):
                    try:
                        usr=jsonData['usr']
                        deviceId=jsonData['oldDevice']

                        tempClient=getDevice(usr,deviceId)
                        if(tempClient!='-1'):
                            msgQueue[tempClient].put('{"type":"exit"}')
                        cbClient=getDevice(usr,'-1')
                        data={"name":"updateDevice","usr":usr,"deviceId":deviceId,"taskId":"-1","newDevice":jsonData['newDevice']}
                        dbQueue.put([data,cbClient])  
                    except Exception as e :
                        logger.info(str(e))
                        pass

                else:
                    pass
        except Exception as e:
            await websocket.close()
            logger.info(str(e))
            try:
                for i in websocket_users:
                    if(i[0]==websocket):
                        delToken(i[1],i[2])
                        websocket_users.remove(i)
                        break
            except Exception as e:pass
            try:websocket_client.remove(websocket)
            except Exception as e:pass
            raise Exception ('exit')
            return 
# 服务器端主逻辑
async def run(websocket, path):
        try:
            await check_user_permit(websocket)
            await recv_user_msg(websocket)
        except websockets.ConnectionClosed:
            print("ConnectionClosed...", path)    # 链接断开
            return
        except websockets.InvalidState:
            print("InvalidState...")    # 无效状态
            return
        except Exception as e:
            logger.info(str(e))
            return
async def msgOut():
    print("信息发送串口开启")
    # 异步调用asyncio.sleep(1):
    while True:
        await asyncio.sleep(0.5)
        try:
            for s in msgQueue:
                while msgQueue.get(s).empty()!=True:
                        msg=msgQueue.get(s).get_nowait()

                        await s.send(msg)  
        except Exception as e:logger.info(str(e))
        pass
def getDevice(usr,deviceID):
    for i in websocket_users:
            if(i[1]==usr and i[2]==deviceID):
                return i[0]
                break
    return '-1'
def initActivity(data):    
    try:
        name = data['name']
        usr=data['usr']
        deviceId=data['deviceId']
        taskId=data['taskId']
        cbClient=getDevice(usr,'-1')
        if(data['name']=='taskUrl'):
            if 'threadList' in data:
                msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":data['name'],"taskId":taskId,"status":'9',"threadList":data['threadList']}))
            else:
                msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":data['name'],"taskId":taskId,"status":'9'}))       
            return
        if(data['name']=='endScheduler'):
            msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":data['name'],"taskId":taskId,"status":'9'}))
            return
        if(data['status']!='9'):
            msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":data['name'],"taskId":taskId,"status":'0'}))           
        else:
            dbQueue.put([data,cbClient])
    except Exception as e:
        logger.info(str(e))                     
def dbHandle():
    while True:
        time.sleep(1)
        if(dbQueue.empty()==False):
            try:
                tempTask=dbQueue.get()
                data=tempTask[0]
                cbClient=tempTask[1]
                name = data['name']
                usr=data['usr']
                deviceId=data['deviceId']
                taskId=data['taskId']
                if name =='initAccount':
                    tbUsr=data['tbUsr']
                    if initAccount(usr,deviceId,tbUsr) ==True:         
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"initAccount","taskId":taskId,"status":'9'}))
                    else:
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"initAccount","taskId":taskId,"status":'0'}))
                    continue
                if name=='initContent':
                    fromDeviceId=data['fromDeviceId']
                    if initContent(usr,fromDeviceId) ==True and data['status']=='9':         
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"initContent","taskId":taskId,"status":'9'}))
                    else:
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"initContent","taskId":taskId,"status":'0'}))
                    continue
                if name =='updateDevice':
                    newDeviceID=data['newDevice']
                    res=updateDevice(usr,deviceId,newDeviceID)
                    if(res[0]==True):
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"updateDevice","taskId":taskId,"status":'9'}))
                    else:
                        msgQueue[cbClient].put(json.dumps({"type":"taskRes","name":"updateDevice","taskId":taskId,"status":res[1]}))
            except Exception as e :
                logger.info(str(e))
                pass                
def initAccount(usr,deviceID,tbUsrs):
    db = pymysql.connect(host="localhost", 
                     user="sylar", 
                     password="!@#c729865c7", 
                     port=3306,# 端口  
                     database="mov", 
                     charset='utf8mb4')
    cursor = db.cursor()
    if(deviceID!="-1"):
            sql='select 1 from app01_account a join app01_accountdevice b on a.usr=b.usr where a.usr=%s and deviceID=%s and expired>now()'
            cursor.execute(sql,(usr,html.escape(deviceID)))
            res=cursor.fetchone()
            if(len(res)>0):
                try:
                    sql='delete from app01_accountdeviceusr where usr=%s and deviceID=%s'
                    cursor.execute(sql,(usr,html.escape(deviceID)))
                    db.commit()
                except:
                    db.rollback()
                    db.close()
                    return False
                try:
                    sql='insert into app01_accountdeviceusr (usr,deviceID,tbUsr) values (%s,%s,%s)'
                    temp=[]
                    for i in tbUsrs:
                        temp.append((usr,html.escape(deviceID),html.escape(i)))
                    cursor.executemany(sql,temp)
                    db.commit()
                except Exception as e:
                    logger.info(str(e))
                    db.rollback()
                    db.close()
                    return False
                try:
                    sql='update app01_accountdevice set initAccount=1 where usr=%s and deviceID=%s'
                    cursor.execute(sql,(usr,html.escape(deviceID)))
                    db.commit()
                except Exception as e:
                    logger.info(str(e))
                    db.rollback()
                    db.close()
                    return False
                db.close()
                return True
    else:
        cursor.close()
        db.close()
        return False
def initContent(usr,deviceID):
    db = pymysql.connect(host="localhost", 
                     user="sylar", 
                     password="!@#c729865c7", 
                     port=3306,# 端口  
                     database="mov", 
                     charset='utf8mb4')
    cursor = db.cursor()
    if(deviceID!="-1"):
        try:
            sql='update app01_accountdevice set initContent=1 where usr=%s and deviceID=%s'
            cursor.execute(sql,(usr,html.escape(deviceID)))
            
            db.commit()
            cursor.close()
            db.close()
            return True
        except:
            cursor.close()
            db.close()
            return False
def updateDevice(usr,oldDeviceID,newDeviceID):
    db = pymysql.connect(host="localhost", 
                     user="sylar", 
                     password="!@#c729865c7", 
                     port=3306,# 端口  
                     database="mov", 
                     charset='utf8mb4')
    cursor = db.cursor()
    try:
        sql='select exChangeTimes from app01_accountdevice where usr=%s and deviceID=%s'
        cursor.execute(sql,(usr,oldDeviceID))
        res=cursor.fetchone()
        if len(res)>0:
            if(res[0]>=4):
                cursor.close()
                db.close()
                return [False,'1']
            else:    
                sql='update app01_accountdevice set exChangeTimes = exChangeTimes+1 where usr=%s and deviceID=%s'
                cursor.execute(sql,(usr,oldDeviceID))
                sql='update app01_accountdevice set deviceID =%s where deviceID=%s and usr=%s'
                cursor.execute(sql,(newDeviceID,oldDeviceID,usr))
                db.commit()
                cursor.close()
                db.close()
                return [True,'0']
        else:
            cursor.close()
            db.close()
            return [False,'0']
    except:
        try:
            db.rollback()
            cursor.close()
            db.close()
            return [False,'0']
        except:
            cursor.close()
            db.close()
            return [False,'0']
def loginAccount(usr,password,deviceID,deviceName,token):
    if token != '-1' and token !='-2':
        redis_conn = getRedis()
        try:
            expiredTime=redis_conn.get(token).split('#')[0]
            expiredTimeInt=int(time.mktime(time.strptime(expiredTime,"%Y-%m-%d %H:%M:%S")))
            cur=int(time.time())
            if(expiredTimeInt-cur)<0:
                return [False,'-3',token]
            else:
                return [True,expiredTime,token]
        except:
            pass
   
    db = pymysql.connect(host="localhost", 
                    user="sylar", 
                    password="!@#c729865c7", 
                    port=3306,# 端口  
                    database="mov", 
                    charset='utf8mb4')
    cursor = db.cursor()
    sql='select count(*) from app01_accountLogin a join app01_account b on a.usr=b.usr where a.usr=%s and a.password=%s and expired>now()'   
    cursor.execute(sql,(usr,password))
    res=cursor.fetchone()
    if(res[0]==0):
        cursor.close()
        db.close()
        return [False,'-1',token]
    if(deviceID=='-1'):
        cursor.close()
        db.close()
        return [True,'0',token]
    sql='select case when expired>now() then 1 else 0 end,expired from app01_account a join app01_accountdevice b on a.usr=b.usr and a.cardNum=b.cardNum join app01_accountLogin c on a.usr=c.usr where c.usr=%s and c.password=%s and deviceID=%s'
    cursor.execute(sql,(usr,password,deviceID))
    res=cursor.fetchone()
    if(res != None and len(res)>0):
        if res[0]==1:
            cursor.close()
            rand_token='-1'
            if token !='-2':
                redis_conn = getRedis()
                rand_token = str(uuid4())
                redis_conn.setex(usr+deviceID+'token',60 * 125,rand_token)
                expiredTime=redis_conn.setex(rand_token,60 * 120,res[1]+'#'+usr+deviceID)
            db.close()
            return [True,res[1],rand_token]
        else:
            try:
                sql='select a.cardNum,a.expired,b.cardType from app01_account a join app01_card b on a.cardNum=b.cardNum where a.expired in (select min(expired) from app01_account a where remainDevice>0 and usr=%s and expired>now() group by usr) and usr=%s and  a.remainDevice>0 limit 1'
                cursor.execute(sql,(usr,usr))
                res=cursor.fetchone()
                if(res==None):
                    return [False,'-2',token]
                cardNum=res[0]  
                expired=res[1]   
                cardType=res[2]        
                sql='update app01_account set remainDevice=remainDevice-1 where cardNum=%s and usr=%s'
                cursor.execute(sql,(cardNum,usr))
                sql='select exchange from app01_cardgen where cardtype=%s'
                cursor.execute(sql,(cardType))
                res=cursor.fetchone()
                if(cardType==0):
                    sql='update app01_accountdevice set cardNum=%s,exChangeTimes=%s where usr=%s and deviceID=%s'
                else:
                    sql='update app01_accountdevice set cardNum=%s,exChangeTimes=%s where usr=%s and deviceID=%s'
                cursor.execute(sql,(cardNum,4-res[0],usr,deviceID))
                db.commit()
                cursor.close()
                rand_token='-1'
                if token != '-2':
                    redis_conn = getRedis()
                    rand_token = uuid4()
                    redis_conn.setex(usr+deviceID+'token',60 * 125,rand_token)
                    expiredTime=redis_conn.setex(rand_token,60 * 120,expired+'#'+usr+deviceID)
                db.close()
                return [True,expired,rand_token]
            except Exception as e:
                cursor.close()
                db.close()
                db.rollback()
                return [False,'-4',token]                
    else:
        try:
            sql='select a.cardNum,a.expired,b.cardType from app01_account a join app01_card b on a.cardNum=b.cardNum where a.expired in (select min(expired) from app01_account a where remainDevice>0 and usr=%s and expired>now() group by usr) and a.usr=%s and a.remainDevice>0 limit 1'
            cursor.execute(sql,(usr,usr))
            res=cursor.fetchone()
            if(res==None):
                cursor.close()
                db.close()
                return [False,'-3',token]
            cardNum=res[0]  
            expired=res[1]         
            cardType=res[2] 
            sql='update app01_account set remainDevice=remainDevice-1 where cardNum=%s and usr=%s'
            cursor.execute(sql,(cardNum,usr))
            sql='select exchange from app01_cardgen where cardtype=%s'
            cursor.execute(sql,(cardType))
            res=cursor.fetchone()
            if(cardType==0):
                sql='insert into app01_accountdevice (usr,deviceID,deviceName,deviceDesc,cardNum,exChangeTimes) values (%s,%s,%s,%s,%s,%s)'
            elif(cardType==1):
                sql='insert into app01_accountdevice (usr,deviceID,deviceName,deviceDesc,cardNum,exChangeTimes) values (%s,%s,%s,%s,%s,%s)'
            elif(cardType==2):
                sql='insert into app01_accountdevice (usr,deviceID,deviceName,deviceDesc,cardNum,exChangeTimes) values (%s,%s,%s,%s,%s,%s)'
            else:
                sql='insert into app01_accountdevice (usr,deviceID,deviceName,deviceDesc,cardNum,exChangeTimes) values (%s,%s,%s,%s,%s,%s)'
            cursor.execute(sql,(usr,deviceID,deviceName,'',cardNum,4-res[0]))
            db.commit()
            cursor.close()
            rand_token='-1'
            if token !=-2:
                redis_conn = getRedis()
                rand_token = str(uuid4())
                redis_conn.setex(usr+deviceID+'token',60 * 125,rand_token)
                expiredTime=redis_conn.setex(rand_token,60 * 120,expired+'#'+usr+deviceID)
            db.close()
            return [True,expired,rand_token]
        except Exception as e:
            logger.info(str(e))
            cursor.close()
            db.close()
            db.rollback()
            return [False,'-4',token]  
def delToken(usr,deviceID):
    redis_conn = getRedis()
    tokenRes=redis_conn.get(usr+deviceID+'token')
    if tokenRes != None:
        redis_conn.delete(tokenRes)
        redis_conn.delete(usr+deviceID+'token')       
def getRedis():
        while True:
            try:
                redisControl = redis.Redis(connection_pool=redis_pool)
                redisControl.ping()
                return redisControl
            except Exception as e:
                continue     
def setup_log(log_name):
    logger = logging.getLogger(log_name)
    log_path = os.path.join("",log_name)
    logger.setLevel(logging.INFO)
    file_handler = TimedRotatingFileHandler(
        filename=log_path, when="MIDNIGHT", interval=1, backupCount=30,encoding='utf-8'
    )
    file_handler.suffix = "%Y-%m-%d.log"
    file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
    file_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(process)d] [%(levelname)s] - %(module)s.%(funcName)s (%(filename)s:%(lineno)d) - %(message)s"
        )
    )
    logger.addHandler(file_handler)
    return logger
if __name__ == '__main__':
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
    logger = setup_log("mylog")
    print("websocket...")
    msgQueue={}
    dbQueue=queue.Queue()
    thread_dbHandle=Thread(target=dbHandle)
    thread_dbHandle.setDaemon(True)
    thread_dbHandle.start()
    redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379,max_connections=1000,decode_responses=True)
    try:
        tasks=[websockets.serve(run, "0.0.0.0", 8712,ping_interval=None,ping_timeout=2,close_timeout=5),msgOut()]
        asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
        asyncio.get_event_loop().run_forever()
    except Exception as e:
        logger.warning(e)





