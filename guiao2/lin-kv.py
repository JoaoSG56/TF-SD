#!/usr/bin/env python3

# 'echo' workload in Python for Maelstrom
# with an addtional custom MyMsg message

import logging
from concurrent.futures import ThreadPoolExecutor
import math
import random
from ms import send, receiveAll, reply, exitOnError

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

dic = {}



def handle(msg):
    # State
    global node_id, node_ids,version, responses,src,key,requests,request_id,locked, locked_requests

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids

        locked_requests = set()
        locked = None

        # requests received
        requests = {}

        request_id = 0
        
        version = 0
        responses = []
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')
    
    elif msg.body.type == 'read':
        logging.info('reading key %s', msg.body.key)

        if msg.src in node_ids:
            if msg.body.key in dic:
                reply(msg,type='read_ok',value=dic[msg.body.key],timestamp=version,request_id=msg.body.request_id)
            else:
                reply(msg,type='error',code=20,timestamp=version,request_id=msg.body.request_id,text='not found')
        else:
            w = math.ceil((len(node_ids)+1)/2)
            src = msg.src
            key = msg.body.key
            quorum = random.sample(node_ids,w)
            requests[str(request_id)] = {"src":src,"msg_id":msg.body.msg_id,"type":"read","key":key,"responses":[]}
                        
            for s in quorum:
                send(node_id,s,type = 'read',key=key,request_id=request_id)
            request_id += 1
        
    elif msg.body.type == 'read_ok':
        #responses.append((msg.body.value,msg.body.timestamp))
        
        requests[str(msg.body.request_id)]["responses"].append((msg.body.timestamp,msg.body.value))
        if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
            # done
            v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
      
            if v[1] is None:
                send(node_id,src,type = 'error',code=20,text='not found')
            else:
                send(node_id,src,type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])
    elif msg.body.type == 'error':
        requests[str(msg.body.request_id)]["responses"].append((msg.body.timestamp,None))  
        if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
            if requests[str(msg.body.request_id)]["type"] == "read":    
                # done
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
               
                if v[1] is None:
                    send(node_id,src,type = 'error',code=20,text='not found')
                else:
                    send(node_id,src,type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])

            elif requests[str(msg.body.request_id)]["type"] == "cas":
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                if v[1] is None:
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",code="20",text="not found")
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="unlock")

                elif v[1] != requests[str(msg.body.request_id)]["from"]:
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",code="22",text="not equal")
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="unlock")
                else:
                    t = v[0]+1
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["to"],timestamp=t)
                        send(node_id,s,type="unlock")
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="cas_ok")

        
    elif msg.body.type == 'write':
        logging.info('writing %s', msg.body.value)

        if msg.src not in node_ids:
            # from client
            logging.info('SENDING lockread request_id: %d',request_id)
            
            w = math.ceil((len(node_ids)+1)/2)
            quorum = random.sample(node_ids,w)
            logging.info("QUORUMS:")
            logging.info(quorum)
            requests[str(request_id)] = {"src":msg.src,"msg_id":msg.body.msg_id,"type":"write","key":msg.body.key,"value":msg.body.value,"responses":[],"quorums":quorum}

            for s in quorum:
                send(node_id,s,type = 'lockread',request_id=request_id,key=msg.body.key)

            request_id += 1

        else:
            if msg.src == locked[0] and msg.body.timestamp > version:
                dic[msg.body.key] = msg.body.value
                version = msg.body.timestamp

    elif msg.body.type == 'lockread_ok':
        if requests[str(msg.body.request_id)]["type"] == "write":
            requests[str(msg.body.request_id)]["responses"].append(msg.body.timestamp)
            if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x)
                v+=1
                for s in requests[str(msg.body.request_id)]["quorums"]:
                    send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["value"],timestamp=v)
                    send(node_id,s,type="unlock")
                send(node_id,requests[str(msg.body.request_id)]["src"],in_reply_to=requests[str(msg.body.request_id)]["msg_id"],type="write_ok")
        elif requests[str(msg.body.request_id)]["type"] == "cas":
            requests[str(msg.body.request_id)]["responses"].append((msg.body.timestamp,msg.body.value))
            if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                if v[1] is None:
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",code="20",text="not found")
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="unlock")

                elif v[1] != requests[str(msg.body.request_id)]["from"]:
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",code="22",text="not equal")
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="unlock")
                else:
                 
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["to"],timestamp=v[0]+1)
                        send(node_id,s,type="unlock")
                    send(node_id,requests[str(msg.body.request_id)]["src"],in_reply_to=requests[str(msg.body.request_id)]["msg_id"],type="cas_ok")
    elif msg.body.type == 'lockread':
        if not locked:
            locked = (msg.src,msg.body.request_id)
            if msg.body.key in dic:
                reply(msg,type="lockread_ok",timestamp=version,request_id=locked[1],value=dic[msg.body.key])
            else:
                reply(msg,type="lockread_ok",timestamp=version,request_id=locked[1],value=None)
        else:
            locked_requests.add((msg.src,msg.body.request_id,msg.body.key))
        
    elif msg.body.type == 'unlock':
        if locked[0] == msg.src:
            locked = None
        reply(msg,type="unlock_ok")
        if len(locked_requests) > 0:
            locked = locked_requests.pop()
            # falta enviar outros campos + adicionar campo key no locked
            if locked[2] in dic:
                send(node_id,locked[0],type='lockread_ok',timestamp=version,request_id=locked[1],value=dic[locked[2]])
            else:
                send(node_id,locked[0],type='lockread_ok',timestamp=version,request_id=locked[1],value=None)
    

    elif msg.body.type == 'cas':
        logging.info("CAS key: %s from: %s to: %s",msg.body.key,getattr(msg.body,'from'),msg.body.to)
        w = math.ceil((len(node_ids)+1)/2)
        quorum = random.sample(node_ids,w)
        requests[str(request_id)] = {"src":msg.src,"msg_id":msg.body.msg_id,"type":"cas","key":msg.body.key,"from":getattr(msg.body,'from'),"to":msg.body.to,"responses":[],"quorums":quorum}

        for s in quorum:
            send(node_id,s,type = 'lockread',request_id=request_id,key=msg.body.key)

        request_id += 1

    
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()

# exitOnError is always optional, but useful for debugging