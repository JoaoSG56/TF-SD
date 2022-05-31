#!/usr/bin/env python

import logging
from asyncio import run, create_task, sleep

from ams import send, receiveAll, reply
from db import DB

logging.getLogger().setLevel(logging.DEBUG)

db = DB(True)

async def handle(msg):
    # State
    global node_id, node_ids
    global db

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')

    elif msg.body.type == 'txn':
        logging.info('executing txn')

        ctx = await db.begin([k for op,k,v in msg.body.txn], msg.src+'-'+str(msg.body.msg_id))
        rs,wv,res = await db.execute(ctx, msg.body.txn)
        if res:
            await db.commit(ctx, wv)

            if msg.src not in node_ids:
                for n in node_ids:
                    send(node_id,n,type="txn_replication",txn=msg.body.txn,wv=wv,res=res,client=msg.src,id=msg.body.msg_id)

        else:
            reply(msg, type='error', code=14, text='transaction aborted')
        db.cleanup(ctx)

    elif msg.body.type == 'txn_replication':

        if msg.src == node_id:
            send(node_id,msg.body.client, type='txn_ok', txn=msg.body.res, in_reply_to=msg.body.id)
        else:
            ctx = await db.begin([k for op,k,v in msg.body.txn], msg.src+'-'+str(msg.body.msg_id))

            await db.commit(ctx, msg.body.wv)

            db.cleanup(ctx)
    else:
        logging.warning('unknown message type %s', msg.body.type)

    


# Main loop
run(receiveAll(handle))
