#!/usr/bin/env python

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB(True) 

async def handle(msg):
    # State
    global node_id, node_ids, db, messages_no_ts, messages_out_of_order, current

    async def process_rec():
        if messages_out_of_order[current]:
            m = messages_out_of_order.pop(current)

            if(m.body.type == 'txn'):
                await process_server(current, m)
            elif(m.body.type == 'txn_replication'):
                await process_other_server(m)

    async def process_server(ts, message):
        global current

        ctx = await db.begin([k for op,k,v in message.body.txn], message.src+'-'+str(message.body.msg_id))
        rs,wv,res = await db.execute(ctx, message.body.txn)
                
        if res:
            await db.commit(ctx, wv)

            if message.src not in node_ids:
                for n in node_ids:
                    send(node_id,n,type="txn_replication",txn=message.body.txn,wv=wv,res=res,client=message.src,id=message.body.msg_id,ts=ts)

        else:
            reply(message, type='error', code=14, text='transaction aborted')

            if message.src not in node_ids:
                for n in node_ids:
                    if n != node_id:
                        send(node_id,n,type="aborted")
        
        current += 1
        db.cleanup(ctx)

        await process_rec()

        
    async def process_other_server(message):
        global current
        ctx = await db.begin([k for op,k,v in message.body.txn], message.src+'-'+str(message.body.id))

        await db.commit(ctx, message.body.wv)
        
        current += 1
        db.cleanup(ctx)

        await process_rec()

    async def aborted():
        current+=1

        await process_rec()

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        messages_no_ts = list()
        messages_out_of_order = dict()
        current = 0
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    elif msg.body.type == 'txn':
        logging.info('executing txn')

        messages_no_ts.append(msg)

        send(node_id, "lin-tso", type="ts")

    elif msg.body.type == 'ts_ok':
        message = messages_no_ts.pop(0)

        if msg.body.ts == current:
            await process_server(current, message)
        else:
            messages_out_of_order[current] = message

    elif msg.body.type == 'txn_replication':

        if msg.src == node_id:
            send(node_id,msg.body.client, type='txn_ok', txn=msg.body.res, in_reply_to=msg.body.id)
        else:
            if msg.body.ts == current:
                await process_other_server(msg)
            else:
                messages_out_of_order[current] = message

    elif msg.body.type == 'aborted':
        await aborted()
        
    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
run(receiveAll(handle))
