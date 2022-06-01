#!/usr/bin/env python

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB(True) 

async def handle(msg):
    global node_id, node_ids, db, messages_no_ts, messages_out_of_order, current

    async def process_transition(message):
        global current, messages_out_of_order

        if message.body.ts == current:

            if message.body.type == 'txn':
                # Locks são adquiridos
                ctx = await db.begin([k for op,k,v in message.body.txn], message.src+'-'+str(message.body.msg_id))

                # Se recebeu pedido do cliente executa a operação            
                rs,wv,res = await db.execute(ctx, message.body.txn)
                        
                if res:
                    # Da commit do resultado da operação
                    await db.commit(ctx, wv)

                    # Envia resultado a todos os nodos
                    if message.src not in node_ids:
                        for n in node_ids:
                            send(node_id,n,type='txn_replication',txn=message.body.txn,wv=wv,res=res,client=message.src,id=message.body.msg_id,ts=message.body.ts)
                else:
                    # Envia aborted caso res esteja vazio
                    if message.src not in node_ids:
                        for n in node_ids:
                            send(node_id,n,type="txn_aborted", client=message.src)
                
                # Liberta locks
                db.cleanup(ctx)

            elif message.body.type == 'txn_replication':
                # Locks são adquiridos
                ctx = await db.begin([k for op,k,v in message.body.txn], message.src+'-'+str(message.body.msg_id))

                # Se for mensagem de replicação da commit do resultado recebido
                await db.commit(ctx, message.body.wv)

                # Liberta locks
                db.cleanup(ctx)

            # Aumenta o current em qualquer caso, mesmo quando é aborted
            current += 1

            # Verifica se alguma mensagem esta em espera
            if current in messages_out_of_order:
                await process_transition(messages_out_of_order.pop(current))

        else:
            # Adiciona message no dict se não esta na sua vez 
            messages_out_of_order.update({message.body.ts:message})

    # Messagem de init
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        messages_no_ts = list()
        messages_out_of_order = dict()
        current = 0
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    # Mensagem com as operações a executar
    elif msg.body.type == 'txn':
        # Adiciona mensagem a queue sem ts
        messages_no_ts.append(msg)

        # Pede ts para poder controlar a ordem
        send(node_id, "lin-tso", type="ts")

    # Mensagem que contém o ts
    elif msg.body.type == 'ts_ok':
        # Remove mensagem de queue sem ts
        message = messages_no_ts.pop(0)

        # Efetuar a transição
        message.body.ts = msg.body.ts
        await process_transition(message)

    # Mensagem para adicionar resultado de operação
    elif msg.body.type == 'txn_replication':
        
        if msg.src == node_id:
            # Se a mensagem for do proprio nodo responde ao cliente com tnx ok
            send(node_id,msg.body.client, type='txn_ok', txn=msg.body.res, in_reply_to=msg.body.id)
        else:
            # Efetuar a transição
            await process_transition(msg)

    elif msg.body.type == 'txn_aborted':

        if msg.src == node_id:
            # Se a mensagem for do proprio nodo responde ao cliente com error
            send(node_id,msg.body.client, type='error', code=14, text='transaction aborted', in_reply_to=msg.body.id)
        else:
            # Efetuar a transição
            await process_transition(msg)
        
    else:
        # Erro
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
