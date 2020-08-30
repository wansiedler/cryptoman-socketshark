import json
import logging

from logging import config
import os
import typing
import asyncio
from datetime import datetime, timedelta

from aiohttp import web, WSMsgType
from aiohttp.web_request import Request
from aiohttp.http_websocket import WSMessage
import redis
import aioredis
from redis import Redis

# # DataFlair #Logging Information
DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(levelname).3s|%(asctime)s|%(filename)s@%(lineno)s> %(message)s',
            'reset': False,
            'datefmt': "%H:%M",
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red',
            },
        },
    },
    'handlers': {
        'console': {
            'level': "DEBUG",
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
        },
    },
    'loggers': {
        'connectors.socketshark': {
            'level': "DEBUG",
            'handlers': ['console'],
            'propagate': False,
        },
    },
}

# logging.config.dictConfig(DEFAULT_LOGGING)

logger = logging.getLogger('connectors.socketshark')

HOST = '0.0.0.0'
PORT = int(os.getenv('SOCKETSHARK_PORT', 9011))
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_password = os.getenv('REDIS_PASSWORD', '')

if redis_password:
    r: Redis = redis.client.StrictRedis(host=redis_host, password=redis_password)
else:
    r: Redis = redis.client.StrictRedis(host=redis_host)

routes = web.RouteTableDef()

sockets: typing.List['Socket'] = []


class Socket(typing.NamedTuple):
    user_id: int
    ws: web.WebSocketResponse
    subscriptions: typing.Set


expire_queue = asyncio.Queue()


# @routes.get(r'/{token:\w+}')
# async def onMessage(request: Request):
#     ws = web.WebSocketResponse()
#     await ws.prepare(request)
#
#     token = request.match_info['token']
#     user_id = int(r.hget('private.tokens', token))
#     if user_id:
#         await expire_queue.put((token, user_id, datetime.now()+timedelta(minutes=5)))
#         row = SocketRow(user_id, ws, set())
#         sockets.append(row)
#     else:
#         return ws
#
#     async for msg in ws:  # type: WSMessage
#         if msg.type == WSMsgType.TEXT:
#             data = json.loads(msg.data)
#             row.subscriptions.add(data['subscribe'])
#         elif msg.type == WSMsgType.ERROR:
#             print('ws connection closed with exception %s' %
#                   ws.exception())
#
#     sockets.remove(row)
#
#     print('websocket connection closed')
#
#     return ws

@routes.get(r'/{token:\w+}')
async def on_message(request: Request):
    websocket = web.WebSocketResponse()
    await websocket.prepare(request)

    token = request.match_info['token']
    user_id = int(r.hget('private.tokens', token))
    if not token or not user_id:
        return web.Response(text='Hello, friend. Youd better go away')
    # await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=5)))
    socket = Socket(user_id, websocket, set())

    sockets.append(socket)

    async for websocket_message in websocket:  # type: WSMessage
        if websocket_message.type == WSMsgType.TEXT:
            logger.debug("adding a subscription")
            data = json.loads(websocket_message.data)
            socket.subscriptions.add(data['subscribe'])
            logger.debug(socket.subscriptions)
        elif websocket_message.type == WSMsgType.ERROR:
            logger.debug('ws connection closed with exception %s' %
                         websocket.exception())

    # sockets.remove(socket)

    logger.debug('websocket connection closed')

    return websocket


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
        # await asyncio.sleep(0.1)


async def reader_private(private, name):
    while True:
        try:
            message = await private.get_json()
            if message['user_id'] in sockets:
                for row in sockets:
                    if message['user_id'] == row.user_id and name in row.subscriptions:
                        await row.ws.send_json(message)
        except:
            pass
        # await asyncio.sleep(0.1)


async def redis_worker(app):
    if redis_password:
        r = await aioredis.create_redis(f'redis://{redis_host}', password=redis_password)
    else:
        r = await aioredis.create_redis(f'redis://{redis_host}')
    chan = await r.subscribe("common.messages")
    common = chan[0]
    chan = await r.subscribe("private.messages")
    private = chan[0]
    chan = await r.subscribe("common.quotes")
    quotes = chan[0]

    asyncio.create_task(reader_common(common, 'common.messages'))
    asyncio.create_task(reader_private(private, 'private.messages'))
    asyncio.create_task(reader_common(quotes, 'common.quotes'))


async def create_expire_task(app):
    asyncio.create_task(expire_task())


if __name__ == '__main__':
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(redis_worker)
    app.on_startup.append(create_expire_task)
    web.run_app(app, host='0.0.0.0', port=9011)
