import json
import typing
import asyncio
from datetime import datetime, timedelta

from aiohttp import web, WSMsgType
from aiohttp.web_request import Request
from aiohttp.http_websocket import WSMessage
import redis
import aioredis

r = redis.Redis()
routes = web.RouteTableDef()

sockets: typing.List['SocketRow'] = []


class SocketRow(typing.NamedTuple):
    user_id: int
    ws: web.WebSocketResponse
    subscriptions: typing.Set


expire_queue = asyncio.Queue()

@routes.get(r'/{token:\w+}')
async def onMessage(request: Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    token = request.match_info['token']
    user_id = int(r.hget('private.tokens', token))
    if user_id:
        await expire_queue.put((token, user_id, datetime.now()+timedelta(minutes=5)))
        row = SocketRow(user_id, ws, set())
        sockets.append(row)
    else:
        return ws

    async for msg in ws:  # type: WSMessage
        if msg.type == WSMsgType.TEXT:
            data = json.loads(msg.data)
            if 'subscribe' in data:
                row.subscriptions.add(data['subscribe'])
            elif 'action' in data:
                r.rpush(data['action'], json.dumps(data['data']))
        elif msg.type == WSMsgType.ERROR:
            print('ws connection closed with exception %s' %
                  ws.exception())

    sockets.remove(row)

    print('websocket connection closed')

    return ws


async def expire_task():
    while True:
        for _ in range(expire_queue.qsize()):
            token, user_id, time = await expire_queue.get()
            if time <= datetime.now():
                r.hdel('private.tokens', token)
                r.hdel('private.users', user_id)
            else:
                await expire_queue.put((token, user_id, time))
        await asyncio.sleep(1)


async def reader_common(common, name):
    while True:
        try:
            message = await common.get_json()
            for row in sockets:
                if name in row.subscriptions:
                    await row.ws.send_json(message)
        except:
            pass
        #await asyncio.sleep(0.1)


async def reader_private(private, name):
    while True:
        try:
            message = await private.get_json()
            if 'user_id' in message:
                for row in sockets:
                    if message['user_id'] == row.user_id and name in row.subscriptions:
                        await row.ws.send_json(message)
        except:
            pass
        #await asyncio.sleep(0.1)


async def redis_worker(app):
    r = await aioredis.create_redis('redis://localhost')
    chan = await r.subscribe("common.messages")
    common = chan[0]
    chan = await r.subscribe("private.messages")
    private = chan[0]
    chan = await r.subscribe("common.quotes")
    quotes = chan[0]
    chan = await r.subscribe("private.signals")
    signals = chan[0]

    asyncio.create_task(reader_common(common, 'common.messages'))
    asyncio.create_task(reader_private(private, 'private.messages'))
    asyncio.create_task(reader_common(quotes, 'common.quotes'))
    asyncio.create_task(reader_private(signals, "private.signals"))


async def create_expire_task(app):
    asyncio.create_task(expire_task())

if __name__ == '__main__':
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(redis_worker)
    app.on_startup.append(create_expire_task)
    web.run_app(app, host='0.0.0.0', port=9011)
