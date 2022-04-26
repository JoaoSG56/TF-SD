#!/usr/bin/env python3

import logging
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from ms import send, receiveAll, reply, exitOnError

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = {}


def handle(msg):
    # State
    global node_id, node_ids,leader, log, currentTerm

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        
        leader = "n0"
        log = []
        currentTerm = 1

        # Volatile state on all servers
        commitIndex = 0
        lastApplied = 0

        # Volatile state on leaders
        nextIndex={}
        matchIndex={}

        logging.info('node %s initialized', node_id)

        def leaderTask():
            time.sleep(0.05)
            for n in node_ids:
                if n!= node_id:
                    if commitIndex >= nextIndex[n]:
                        send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=log[nextIndex[n]-1],entries=log[nextIndex[n]:],leaderCommit=commitIndex)



        # matchindex --> commitIndex
        # nextIndex -> len(log)
        if node_id == leader:
            for n in node_ids:
                if n!=node_id:
                    nextIndex[n] = 1
                    # send initial heartbeat
                    send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=None,entries=[],leaderCommit=commitIndex)
        threading.Thread(target=leaderTask).start()
        reply(msg, type='init_ok')
    
    elif node_id == leader:
        if msg.body.type == 'AppendEntries_ok':
            if msg.body.success:
                nextIndex[msg.src] = len(log)
            pass
        else:
            log.append((currentTerm,msg))
            
            # for n in node_ids:
            #     if n!= node_id:
            #         send(node_id,n,type="AppendEntries",term=currentTerm,leaderId=node_id,prevLogIndex=nextIndex[n]-1,prevLogTerm=log[nextIndex[n]-1],entries=log[nextIndex[n]-1:],leaderCommit=commitIndex)


    elif msg.body.type == 'AppendEntries':
        # 1
        if msg.body.term < currentTerm:
            reply(msg,type="AppendEntries_ok",success=False,term=currentTerm,prevLogIndex=len(log))
        
        # 2
        elif len(log) < msg.body.prevLogIndex or log[msg.body.prevLogIndex][0] != msg.body.prevLogTerm:
            reply(msg,type="AppendEntries_ok",success=False,term=currentTerm,prevLogIndex=len(log))


        # heartbeat
        elif msg.body.entries == []:
            # empty append entries
            reply(msg,type="AppendEntries_ok",term=currentTerm,success=(msg.body.term>=currentTerm), prevLogIndex=len(log))
            currentTerm = msg.body.term
        
            # 5???????'
            if currentTerm<msg.body.term:
                commitIndex = min(msg.body.term,len(log)-1) # ???
            
        #
        elif msg.body.leaderCommit > lastApplied:
            # apply log[lastApplied]
            if msg.body.term > currentTerm:
                currentTerm = msg.body.term
            reply(msg,type="AppendEntries_ok",success=True,term=currentTerm,prevLogIndex=len(log))
        
  
    
    
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

