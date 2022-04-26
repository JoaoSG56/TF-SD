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



def handle(msg):
    # State
    global node_id, node_ids,version, responses,requests,request_id,locked, dic
    
    # def handleRead(msg):
    #     v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])

    #     if v[1] is None:
    #     # Se o valor do nodo que tem maior versao for none então não existe esse valor
    #         send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',code=20,text='not found')
    #     else:
    #         # Se tiver valor, então responde com read_ok para o cliente
    #         send(node_id,requests[str(msg.body.request_id)]['src'],type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])


    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids

        locked = None

        # key: (timestamp,value)
        dic = {}

        # requests received
        requests = {}

        request_id = 0
        
        version = 0
        responses = []
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')
    
    # Tipo read
    elif msg.body.type == 'read':
        logging.info('reading key %s', msg.body.key)

        if msg.src in node_ids:
            # Se mensagem vem de outro Servidor
            if msg.body.key in dic:
                # Se houver a key no dic, responde com o valor
                reply(msg,type='read_ok',value=dic[msg.body.key],request_id=msg.body.request_id)
            else:
                # Se não houver responde com erro not found
                reply(msg,type='error',code=20,request_id=msg.body.request_id,text='not found')
        else:
            # Se mensagem vier de um cliente
            w = math.ceil((len(node_ids)+1)/2)
            src = msg.src
            key = msg.body.key

            # calcula um quorum com w nodes (o próprio servidor pode ou não fazer parte do mesmo)
            quorum = random.sample(node_ids,w)

            # cria entrada na tabela de requests com a chave do request_id
            requests[str(request_id)] = {"src":src,"msg_id":msg.body.msg_id,"type":"read","key":key,"responses":[],"quorums":quorum}
                        
            for s in quorum:
                # para cada nodo no quorum envia pedido de leitura
                # a resposta de cada nodo é feita no 'read_ok' ou no 'error'
                send(node_id,s,type = 'lockread',key=key,request_id=request_id)
            request_id += 1
        
    elif msg.body.type == 'read_ok':
        # insere par de resposta (timestamp,value) no campo de respostas de um dado request do cliente (dado pelo request_id)
        # requests[str(msg.body.request_id)]["responses"].append(msg.body.value)
        # if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
        #     # Já recebeu as respostas de todos os nodos
        #     #handleRead(msg)
        #     # Calculo do timestamp maior:
        #     v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
      
        #     if v[1] is None:
        #         # Se o valor do nodo que tem maior versao for none então não existe esse valor
        #         send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=20,text='not found')
        #     else:
        #         # Se tiver valor, então responde com read_ok para o cliente
        #         send(node_id,requests[str(msg.body.request_id)]['src'],type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])
        requests[str(msg.body.request_id)]["responses"].append(msg.body.value)
        if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
            if requests[str(msg.body.request_id)]["type"] == "cas":
                # Caso o type seja cas
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                if v[1] is None:
                    # Caso o valor seja None, dá reply com not found e envia unlocks aos servidores
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="20",text="not found")
                    #for s in requests[str(msg.body.request_id)]["quorums"]:
                        #send(node_id,s,type="unlock",request_id=msg.body.request_id)

                elif v[1] != requests[str(msg.body.request_id)]["from"]:
                    # Caso o valor seja diferente do que esta no from, da reply com not equal e envia unlocks aos servidores
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="22",text="not equal")
                    #for s in requests[str(msg.body.request_id)]["quorums"]:
                        #send(node_id,s,type="unlock",request_id=msg.body.request_id)
                else:
                    # escreve nos quorums e dá unlock
                    requests[str(msg.body.request_id)]["responses"] = []
                    requests[str(msg.body.request_id)]["readDone"] = True
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type = 'lockread',request_id=msg.body.request_id,key=requests[str(msg.body.request_id)]["key"])
#                        send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["to"],timestamp=v[0]+1)
                        #send(node_id,s,type="unlock")
    elif msg.body.type == 'error':
        # insere par de resposta (timestamp,None) no campo de respostas de um dado request do cliente (dado pelo request_id)
        if msg.body.code == 11:
            # -2 caso seja code 11
            requests[str(msg.body.request_id)]["responses"].append((-2,None))
        else:
            requests[str(msg.body.request_id)]["responses"].append((-1,None))
                
        # if msg.body.code == 11 and requests[str(msg.body.request_id)]["error"] != True:
        #     requests[str(msg.body.request_id)]["error"]=True
        #     for s in requests[str(msg.body.request_id)]["locked_servers"]:
        #         if s != node_id:
        #             send(node_id,s,type="unlock")
        #     send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=11,text='not available')

        if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
            # Já recebeu as respostas de todos os nodos
            flag = False
            for r in requests[str(msg.body.request_id)]["responses"]:
                if r[0] == -2:
                    flag = True
            if flag:
                for s in requests[str(msg.body.request_id)]["quorums"]:
                    send(node_id,s,type="unlock",request_id=msg.body.request_id)
                send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=11,text='not available')
   
            elif requests[str(msg.body.request_id)]["type"] == "read":
                # type == read

                #handleRead(msg)
                v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])

                for s in requests[str(msg.body.request_id)]["quorums"]:
                    send(node_id,s,type="unlock",request_id=msg.body.request_id)

                if v[1] is None:
                    send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=20,text='not found')
                else:
                    send(node_id,requests[str(msg.body.request_id)]['src'],type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])

            elif requests[str(msg.body.request_id)]["type"] == "cas":
                if requests[str(msg.body.request_id)]["readDone"]:
# Caso o type seja cas
                    v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                    # escreve nos quorums e dá unlock
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["to"],timestamp=v[0]+1)
                        #send(node_id,s,type="unlock")
                    send(node_id,requests[str(msg.body.request_id)]["src"],type="cas_ok")
                else:
                    # Caso o type seja cas
                    v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                    if v[1] is None:
                        # Caso o valor seja None, dá reply com not found e envia unlocks aos servidores
                        send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="20",text="not found")
                        #for s in requests[str(msg.body.request_id)]["quorums"]:
                            #send(node_id,s,type="unlock",request_id=msg.body.request_id)

                    elif v[1] != requests[str(msg.body.request_id)]["from"]:
                        # Caso o valor seja diferente do que esta no from, da reply com not equal e envia unlocks aos servidores
                        send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="22",text="not equal")
                        #for s in requests[str(msg.body.request_id)]["quorums"]:
                            #send(node_id,s,type="unlock",request_id=msg.body.request_id)
                    else:
                        # escreve nos quorums e dá unlock
                        requests[str(msg.body.request_id)]["responses"] = []
                        requests[str(msg.body.request_id)]["readDone"] = True
                        for s in requests[str(msg.body.request_id)]["quorums"]:
                            send(node_id,s,type = 'lockread',request_id=msg.body.request_id,key=requests[str(msg.body.request_id)]["key"])
                

        
    elif msg.body.type == 'write':
        logging.info('writing %s', msg.body.value)

        if msg.src not in node_ids:
            # from client
            logging.info('SENDING lockread request_id: %d',request_id)
            
            w = math.ceil((len(node_ids)+1)/2)
            quorum = random.sample(node_ids,w)
            logging.info("QUORUMS:")
            logging.info(quorum)
            requests[str(request_id)] = {"src":msg.src,"msg_id":msg.body.msg_id,"type":"write","key":msg.body.key,"value":msg.body.value,"responses":[],"quorums":quorum,"error":False}

            for s in quorum:
                send(node_id,s,type = 'lockread',request_id=request_id,key=msg.body.key)

            request_id += 1

        else:
            # from other node
            if msg.src == locked[0]:
                if (msg.body.key in dic and msg.body.timestamp > dic[msg.body.key][0]) or msg.body.key not in dic:
                # caso o node tenha o lock adquirido e a sua versão (do node) for maior, então atualiza a versão e o dic
                    dic[msg.body.key] = (msg.body.timestamp,msg.body.value)
                    if msg.body.timestamp > version:
                        version = msg.body.timestamp
                locked = None

    elif msg.body.type == 'lockread_ok':
        # caso seja obtido o lock
        requests[str(msg.body.request_id)]["responses"].append(msg.body.value)
        if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
            flag = False
            for r in requests[str(msg.body.request_id)]["responses"]:
                if r[0] == -2:
                    flag = True
            if flag:
                for s in requests[str(msg.body.request_id)]["quorums"]:
                    send(node_id,s,type="unlock",request_id=msg.body.request_id)
                send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=11,text='not available')


            elif requests[str(msg.body.request_id)]["type"] == "write":
                # insere o timestamp nas respostas e quando as obtiver todas, escreve nos nodos e responde ao cliente
                if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
                    v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["value"],timestamp=v[0]+1)
                        #send(node_id,s,type="unlock")
                    send(node_id,requests[str(msg.body.request_id)]["src"],in_reply_to=requests[str(msg.body.request_id)]["msg_id"],type="write_ok")
            elif requests[str(msg.body.request_id)]["type"] == "cas":
                # insere o (timestamp,value) nas respostas e quando as obtiver todas, escreve nos nodos, dá unlock e responde ao cliente
                if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
                    v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])
                    if v[1] is None:
                        send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="20",text="not found")
                        for s in requests[str(msg.body.request_id)]["quorums"]:
                            send(node_id,s,type="unlock",request_id=msg.body.request_id)

                    elif v[1] != requests[str(msg.body.request_id)]["from"]:
                        send(node_id,requests[str(msg.body.request_id)]["src"],type="error",in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code="22",text="not equal")
                        for s in requests[str(msg.body.request_id)]["quorums"]:
                            send(node_id,s,type="unlock",request_id=msg.body.request_id)
                    else:
                    
                        for s in requests[str(msg.body.request_id)]["quorums"]:
                            send(node_id,s,type="write",key=requests[str(msg.body.request_id)]["key"],value=requests[str(msg.body.request_id)]["to"],timestamp=v[0]+1)
                            #send(node_id,s,type="unlock")
                        send(node_id,requests[str(msg.body.request_id)]["src"],in_reply_to=requests[str(msg.body.request_id)]["msg_id"],type="cas_ok")
            elif requests[str(msg.body.request_id)]["type"] == "read":
                # type == read
                if len(requests[str(msg.body.request_id)]["responses"]) == math.ceil((len(node_ids)+1)/2):
                    v = max(requests[str(msg.body.request_id)]["responses"], key = lambda x : x[0])

                    for s in requests[str(msg.body.request_id)]["quorums"]:
                        send(node_id,s,type="unlock",request_id=msg.body.request_id)

                    if v[1] is None:
                        send(node_id,requests[str(msg.body.request_id)]['src'],type = 'error',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],code=20,text='not found')
                    else:
                        send(node_id,requests[str(msg.body.request_id)]['src'],type = 'read_ok',in_reply_to=requests[str(msg.body.request_id)]["msg_id"],value=v[1])

                
    elif msg.body.type == 'lockread':
        # caso um nodo queira um lock e ler o valor de uma key
        if not locked:
            # se não houver já um lock, então dá lock
            locked = (msg.src,msg.body.request_id)
            # responder com o valor da key
            if msg.body.key in dic:
                reply(msg,type="lockread_ok",request_id=locked[1],value=dic[msg.body.key])
            else:
                reply(msg,type="lockread_ok",request_id=locked[1],value=(-1,None))
        else:
            # insere numa queue de locks
            #locked_requests.append((msg.src,msg.body.request_id,msg.body.key))
            reply(msg,type='error',code=11,request_id=msg.body.request_id)

    elif msg.body.type == 'unlock':
        # unlock vindo de um nodo
        if locked is not None and locked[0] == msg.src and locked[1] == msg.body.request_id:
            # se o unlock for feito pelo nodo que o possui , dá unlock e responde com unlock_ok
            locked = None
            reply(msg,type="unlock_ok",request_id=msg.body.request_id)
        # if len(locked_requests) > 0:
        #     # se houver pedidos de lock na queue, dá pop e responde conforme o pedido
        #     locked = locked_requests.pop(0)
        #     # falta enviar outros campos + adicionar campo key no locked
        #     if locked[2] in dic:
        #         send(node_id,locked[0],type='lockread_ok',request_id=locked[1],value=dic[locked[2]])
        #     else:
        #         send(node_id,locked[0],type='lockread_ok',request_id=locked[1],value=(-1,None))
    

    elif msg.body.type == 'cas':
        logging.info("CAS key: %s from: %s to: %s",msg.body.key,getattr(msg.body,'from'),msg.body.to)
        w = math.ceil((len(node_ids)+1)/2)
        quorum = random.sample(node_ids,w)

        # cria entrada na tabela de requests
        requests[str(request_id)] = {"src":msg.src,"msg_id":msg.body.msg_id,"type":"cas","key":msg.body.key,"from":getattr(msg.body,'from'),"to":msg.body.to,"responses":[],"quorums":quorum,"error":False,"readDone":False}

        # envia lock and read ao quorum que depois é tratado no 'if type==lockread_ok'
        for s in quorum:
            #send(node_id,s,type = 'lockread',request_id=request_id,key=msg.body.key)
            send(node_id,s,type = 'read',request_id=request_id,key=msg.body.key)

        request_id += 1

    
    
# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()

# exitOnError is always optional, but useful for debugging