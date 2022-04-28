#!/usr/bin/env python3
import logging
import math
from mimetypes import init
from nis import match
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
from ms import retransmit, send, receiveAll, reply, exitOnError, send2

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

def maiorComum(matchIndex):
    dic = {}
    for value in matchIndex.values():
        if value in dic:
            dic[value] += 1
        else:
            dic[value] = 1

    list = sorted(dic.items(),key=lambda x: -x[0])
    sum = 0
    m = math.floor(len(matchIndex.keys())/2)+1
    for i in list:
        sum += i[1]
        if sum>=m:
            return i[0]

def handle(msg):
    # State
    global node_id, node_ids,leader, log, currentTerm, db, commitIndex, nextIndex, matchIndex, lastApplied

    def apply(request):
        if request.body.type == 'write':
            db[request.body.key] = request.body.value
        elif request.body.type == 'cas':
            if request.body.key in db and getattr(request.body,"from") == db[request.body.key]:
                db[request.body.key] = request.body.to

    def leaderApply(request):
        if request.body.type == 'write':
            db[request.body.key] = request.body.value
            reply(request,type='write_ok')
        elif request.body.type == 'read':
            if request.body.key not in db:
                reply(request,type='error',code=20,text='not found')
            else:
                reply(request,type='read_ok',value=db[request.body.key])
        elif request.body.type == 'cas':
            if request.body.key not in db:
                reply(request,type='error',code=20,text='not found')
            elif getattr(request.body,'from') != db[request.body.key]:
                reply(request,type='error',code=22,text='not equal')
            else:
                db[request.body.key] = request.body.to
                reply(request,type='cas_ok')

    def sendAppendEntry():
        for n in node_ids:
            if n!= node_id:
                if len(log)-1 >= nextIndex[n]:
                    send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=log[nextIndex[n]-1][0],entries=log[nextIndex[n]:],leaderCommit=commitIndex)

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids

        db = {}
        
        leader = "n0"
        log = []
        currentTerm = 1

        # Volatile state on all servers
        commitIndex = -1
        lastApplied = -1

        # Volatile state on leaders
        nextIndex={}
        matchIndex={}
        inited = False

        logging.info('node %s initialized', node_id)

        def leaderTask():
            while True:
                time.sleep(1)
                sendAppendEntry()


        # matchindex --> commitIndex
        # nextIndex -> len(log)
        if node_id == leader:
            for n in node_ids:
                if n!=node_id:
                    nextIndex[n] = 0
                    matchIndex[n] = -1
                    # send initial heartbeat
                    send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=None,entries=[],leaderCommit=commitIndex)
        
            # threading.Thread(target=leaderTask).start()
        reply(msg, type='init_ok')
    
    elif node_id == leader:
        if msg.body.type == 'AppendEntries_ok':
            if msg.body.success:
                logging.info("[DEBUG] nextIndex: "+ str(msg.body.nextIndex) + " Match index: " + str(msg.body.matchIndex))
                nextIndex[msg.src] = msg.body.nextIndex
                matchIndex[msg.src] = msg.body.matchIndex

                if ((n:=maiorComum(matchIndex))>commitIndex):
                    commitIndex = n

                if commitIndex>lastApplied:
                    lastApplied+=1
                    logging.info("[DEBUG] LastApplied: " + str(lastApplied) + " len logs : " + str(len(log)))
                    leaderApply(log[lastApplied][1])

            elif msg.body.inconsistency:
                nextIndex[msg.src]-=1
                logging.info("[DEBUG] ALGO ESTá MAL")

        else:
            log.append((currentTerm,msg))
            sendAppendEntry()
            # for n in node_ids:
            #     if n!= node_id:
            #         send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=log[nextIndex[n]-1],entries=log[nextIndex[n]-1:],leaderCommit=commitIndex)

    elif msg.src not in node_ids:
        reply(msg,type='error',code='11')
        # if msg.body.type=='read':
        #     send2(msg.src,leader,msg_id=msg.body.msg_id,type=msg.body.type,key=msg.body.key)
        # elif msg.body.type=='write':
        #     send2(msg.src,leader,msg_id=msg.body.msg_id,type=msg.body.type,key=msg.body.key,value=msg.body.value)
        # elif msg.body.type=='cas':
        #     send(msg.src,leader,msg_id=msg.body.msg_id,**msg.body)



        
        #reply(msg,type='error',code='11')
    elif msg.body.type == 'AppendEntries':
        # 1
        logging.info("[DEBUG TEMP] tamanho lista " + str(len(log)) + " : " + str(msg.body.prevLogIndex))
        if msg.body.term < currentTerm:
            logging.info("[DEBUG] termo e menor do que o current")
            reply(msg,type="AppendEntries_ok",success=False,term=currentTerm,inconsistency=False,nextIndex=len(log),matchIndex=len(log)-1)

        # 2
        elif log != [] and msg.body.prevLogIndex >= 0 and(len(log) <= msg.body.prevLogIndex or log[msg.body.prevLogIndex][0] != msg.body.prevLogTerm):
            logging.info("[DEBUG] nao existe ")
            logging.info("[IMPORTANT DEBUG] len log: " + str(len(log)) + " | prevlogindex: " + str(msg.body.prevLogIndex) + " | prevlogterm: " + str(msg.body.prevLogTerm))
            reply(msg,type="AppendEntries_ok",success=False,term=currentTerm,inconsistency=True,nextIndex=len(log),matchIndex=len(log)-1)

        else:
            # 3
            # if msg.body.prevLogIndex < len(log) and log[msg.body.prevLogIndex][0] != msg.body.prevLogTerm:
            #     logging.info("[DEBUG] Conflito entre termos")
            #     log = log[:msg.body.prevLogIndex] + msg.body.entries

            # #
            # # 4
            # else:
            logging.info("[DEBUG] appending entries")
            log.extend(msg.body.entries)

            # 5
            if msg.body.leaderCommit > commitIndex:
                logging.info("[DEBUG] leadercommit > commitindex ")
                commitIndex = min(msg.body.leaderCommit,len(log)-1)

                if commitIndex > lastApplied:
                    lastApplied+=1
                    apply(log[lastApplied][1])
            reply(msg,type="AppendEntries_ok",success=True,term=currentTerm,inconsistency=False,nextIndex=len(log),matchIndex=len(log)-1)
            
  
    
    
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

