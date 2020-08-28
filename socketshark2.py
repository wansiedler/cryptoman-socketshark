#!/usr/bin/env python

import asyncio
import json
import logging
from logging import config
import os
import sys
import traceback
import typing
from datetime import datetime, timedelta

import aioredis
import redis
from aiohttp import web, WSMsgType, WSMessage
from aiohttp.web_request import Request
from redis import Redis

LOGLEVEL = os.environ.get('LOGLEVEL', 'debug').upper()
log_folder = "/var/log/"

date_format_verbose = "%m-%d %H:%M:%S"
date_format_simple = "%H:%M"

format_simple = '%(log_color)s%(levelname).3s|%(asctime)s|%(filename)s@%(lineno)s> %(message)s'

formatter_colored = {
    '()': 'colorlog.ColoredFormatter',
    'format': format_simple,
    'reset': False,
    'datefmt': date_format_simple,
    'log_colors': {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red',
    },
}

formatter_simple = {
    'format': format_simple,
    'datefmt': '%H:%M'
}

# # DataFlair #Logging Information
DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': formatter_simple,
        'colored': formatter_colored
    },
    'handlers': {
        'console': {
            'level': LOGLEVEL,
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
        },
        'file.socketshark': {
            'level': LOGLEVEL,
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 5,  # 5 MB
            'backupCount': 3,
            'filename': log_folder + 'django.socketshark.debug.log',
            'formatter': 'colored',
        },
    },
    'loggers': {
        'connectors.socketshark': {
            'level': LOGLEVEL,
            'handlers': ['file.socketshark', 'console'],
            'propagate': True,
        },
    },
}

logging.config.dictConfig(DEFAULT_LOGGING)

logger = logging.getLogger('connectors.socketshark')

HOST = '0.0.0.0'
PORT = int(os.getenv('SOCKETSHARK_PORT', 9011))
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_password = os.getenv('REDIS_PASSWORD', '')

if redis_password:
    redis_access: Redis = redis.client.StrictRedis(host=redis_host, password=redis_password)
else:
    redis_access: Redis = redis.client.StrictRedis(host=redis_host)

routes = web.RouteTableDef()
sockets: typing.List['Socket'] = []


# expire_queue = asyncio.Queue()


class Socket(typing.NamedTuple):
    user_id: int
    ws: web.WebSocketResponse
    subscriptions: typing.Set


async def redis_worker(app):
    logger.debug('redis worker initialized')

    if redis_password:
        redis_worker_access = await aioredis.create_redis(f'redis://{redis_host}', password=redis_password)
    else:
        redis_worker_access = await aioredis.create_redis(f'redis://{redis_host}')

    chan = await redis_worker_access.subscribe("common.messages")
    common = chan[0]
    chan = await redis_worker_access.subscribe("private.messages")
    private = chan[0]
    chan = await redis_worker_access.subscribe("common.quotes")
    quotes = chan[0]

    asyncio.create_task(reader_common(common, 'common.messages'))
    asyncio.create_task(reader_private(private, 'private.messages'))
    asyncio.create_task(reader_common(quotes, 'common.quotes'))


async def reader_common(common, name):
    logger.debug('   reader common initialized')
    while True:
        message = await common.get_json()
        await logger.debug(message)
        # logger.debug(len(sockets))
        for socket in sockets:
            await socket.ws.send_json(message)
            await asyncio.sleep(0.1)


async def reader_private(private, name):
    logger.debug('   reader private initialized')
    while True:
        message = await private.get_json()
        # logger.debug(message)
        # if message['user_id'] in sockets:
        for socket in sockets:
            if message['user_id'] == socket.user_id and name in socket.subscriptions:
                await socket.ws.send_json(message)
                await asyncio.sleep(0.1)


@routes.get(r'/{token:\w+}')
async def on_message(request: Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    token = request.match_info['token']
    user_id = int(redis_access.hget('private.tokens', token))
    if not token or not user_id:
        logger.debug(f"no token ({token}) or user!")
        return web.Response(text='Hello, friend. Youd better go away')
    logger.debug('token:' + token)
    logger.debug('user_id:' + str(user_id))
    # await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=5)))
    row = Socket(user_id, ws, set())

    sockets.append(row)

    async for msg in ws:  # type: WSMessage
        # logger.debug(msg.type)
        # logger.debug(msg.data)
        if msg.type == WSMsgType.TEXT:
            data = json.loads(msg.data)
            row.subscriptions.add(data['subscribe'])
        elif msg.type == WSMsgType.ERROR:
            logger.debug('ws connection closed with exception %s' %
                         ws.exception())

    # sockets.remove(row)

    logger.debug('websocket connection closed')

    return ws

    #
    # token = request.match_info.get('token', "Anonymous")
    # if not token:
    #     print("no token! byby")
    #     return 0;
    # logger.debug('token:' + token)
    # if token == "hellofriend":
    #     user_id = 1000
    # else:
    #     db_token = redis_access.hget('private.tokens', token)
    #     if not db_token:
    #         print(f"no token ({token}) or user!")
    #         return web.Response(text='Hello, friend. Youd better go away')
    #     user_id = int(db_token)
    #     if not user_id:
    #         print(f"no user ({user_id})!")
    #         return web.Response(text=f"no user ({user_id})!")
    #
    # ws = web.WebSocketResponse()
    # await ws.prepare(request)
    #
    # # await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=10)))
    # row = SocketRow(user_id, ws, set())
    # sockets.append(row)
    #
    # async for msg in ws:  # type: WSMessage
    #     if msg.type == WSMsgType.TEXT:
    #         data = json.loads(msg.data)
    #         row.subscriptions.add(data['subscribe'])
    #     elif msg.type == WSMsgType.ERROR:
    #         print('ws connection closed with exception %s' %
    #               ws.exception())
    #
    # sockets.remove(row)
    #
    # return ws


# async def create_expire_task(app):
#     logger.debug('create expire task initialized')
#     asyncio.create_task(expire_task())
#
#
# async def expire_task():
#     logger.debug('expire task initialized')
#     while True:
#         for _ in range(expire_queue.qsize()):
#             token, user_id, time = await expire_queue.get()
#             if time <= datetime.now():
#                 logger.debug(f"deleted token {token}")
#                 logger.debug(f"deleted user {user_id}")
#                 redis_access.hdel('private.tokens', token)
#                 redis_access.hdel('private.users', user_id)
#             else:
#                 await expire_queue.put((token, user_id, time))
#         await asyncio.sleep(0.1)


async def test(app):
    return web.Response(text='Test handle')


def start():
    app = web.Application()
    app.router.add_route('GET', '/', on_message)
    app.router.add_route('GET', '/test', test)
    app.router.add_routes(routes)
    app.on_startup.append(redis_worker)
    # app.on_startup.append(create_expire_task)

    logger.debug('Starting Socketshark!')
    web.run_app(app, host=HOST, port=PORT)

    # from os.path import getmtime
    #
    # watched_files = [__file__]
    # watched_files_mtimes = [(f, getmtime(f)) for f in watched_files]
    # while True:
    #     for f, mtime in watched_files_mtimes:
    #         if getmtime(f) != mtime:
    #             # os.execv(__file__, sys.argv)
    #             logger.debug('Shutdown')
    #             await app.shutdown()
    #             logger.debug("Start cleaning up")
    #             await app.cleanup()
    #             os.execv(sys.executable, ['python'] + sys.argv)
    #         else:
    #             logger.debug('Starting Socketshark!')
    #             web.run_app(app, host=HOST, port=PORT)


if __name__ == '__main__':
    start()
