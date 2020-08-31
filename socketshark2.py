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
#
# DEFAULT_LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'formatters': {
#         'coloreddd': {
#             '()': 'colorlog.ColoredFormatter',
#             # 'format': '%(log_color)s%(levelname).3s|%(asctime)s|%(filename)s@%(lineno)s> %(message)s',
#             # 'reset': False,
#             # 'datefmt': "%H:%M",
#             # 'log_colors': {
#             #     'DEBUG': 'cyan',
#             #     'INFO': 'green',
#             #     'WARNING': 'yellow',
#             #     'ERROR': 'red',
#             #     'CRITICAL': 'red',
#             # },
#         },
#     },
#     'handlers': {
#         'console': {
#             'level': "DEBUG",
#             'class': 'logging.StreamHandler',
#             # 'formatter': 'coloreddd',
#         },
#     },
#     'loggers': {
#         'connectors.socketshark': {
#             'level': "DEBUG",
#             'handlers': ['console'],
#             'propagate': False,
#         },
#     },
# }
#
# logging.config.dictConfig(DEFAULT_LOGGING)
#
# logger = logging.getLogger('connectors.socketshark')

import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname).3s|%(asctime)s|%(filename)s@%(lineno)s> %(message)s'))
logger = colorlog.getLogger('')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

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

expire_queue = asyncio.Queue()


class Socket(typing.NamedTuple):
    user_id: int
    websocket_response: web.WebSocketResponse
    subscriptions: typing.Set


async def redis_worker(app):
    logger.info('redis worker initialized')

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

    # asyncio.create_task(reader_common(common, 'common.messages'))
    # asyncio.create_task(reader_private(private, 'private.messages'))
    asyncio.create_task(reader_common(quotes, 'common.quotes'))


async def reader_common(common, name):
    logger.info('   reader common initialized')
    while True:
        message = await common.get_json()
        for socket in sockets:
            try:
                await asyncio.shield(socket.websocket_response.send_json(message))
                await asyncio.sleep(0.1)
            except BaseException as e:
                logger.info("asyncio socket.send() raised exception infinite no more %s" % e)
                pass


async def reader_private(private, name):
    logger.info('   reader private initialized')
    while True:
        message = await private.get_json()
        # logger.info(message)
        # if message['user_id'] in sockets:
        for socket in sockets:
            if message['user_id'] == socket.user_id and name in socket.subscriptions:
                await socket.websocket_response.send_json(message)
                await asyncio.sleep(0.1)


#
@routes.get(r'/{token:\w+}')
async def on_message(request: Request):
    websocket_response = web.WebSocketResponse()
    await websocket_response.prepare(request)

    user_id = 0
    # token = request.match_info['token']
    # user_id = int(redis_access.hget('private.tokens', token))
    # if not token or not user_id:
    #     logger.info(f"no token ({token}) or user!")
    #     return web.Response(text='Hello, friend. Youd better go away')
    # logger.info('token:' + token)
    # logger.info('user_id:' + str(user_id))

    # await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=5)))
    socket = Socket(user_id, websocket_response, set())

    sockets.append(socket)

    async for websocket_message in websocket_response:  # type: WSMessage
        if websocket_message.type == WSMsgType.TEXT:
            logger.info("adding a subscription")
            data = json.loads(websocket_message.data)
            socket.subscriptions.add(data['subscribe'])
            logger.info(socket.subscriptions)
        elif websocket_message.type == WSMsgType.ERROR:
            logger.info('ws connection closed with exception %s' %
                        websocket_response.exception())

    sockets.remove(socket)

    logger.info('websocket connection closed')

    return websocket_response


async def create_expire_task(app):
    logger.info('create expire task initialized')
    asyncio.create_task(expire_task())


async def expire_task():
    logger.info('expire task initialized')
    while True:
        for _ in range(expire_queue.qsize()):
            token, user_id, time = await expire_queue.get()
            if time <= datetime.now():
                logger.info(f"deleted token {token}")
                logger.info(f"deleted user {user_id}")
                redis_access.hdel('private.tokens', token)
                redis_access.hdel('private.users', user_id)
            else:
                await expire_queue.put((token, user_id, time))
        await asyncio.sleep(0.1)


#
# async def test(app):
#     return web.Response(text='Test handle')


def start():
    try:
        app = web.Application()
        # app.router.add_route('GET', '/', on_message)
        # app.router.add_route('GET', '/test', test)
        app.router.add_routes(routes)
        app.on_startup.append(redis_worker)
        # app.on_startup.append(create_expire_task)

        logger.info('Starting Socketshark!')
        web.run_app(app, host=HOST, port=PORT)
    except Exception as e:  # ==> except:
        print(f"oops! {e}")

    # from os.path import getmtime
    #
    # watched_files = [__file__]
    # watched_files_mtimes = [(f, getmtime(f)) for f in watched_files]
    # while True:
    #     for f, mtime in watched_files_mtimes:
    #         if getmtime(f) != mtime:
    #             # os.execv(__file__, sys.argv)
    #             logger.info('Shutdown')
    #             await app.shutdown()
    #             logger.info("Start cleaning up")
    #             await app.cleanup()
    #             os.execv(sys.executable, ['python'] + sys.argv)
    #         else:
    #             logger.info('Starting Socketshark!')
    #             web.run_app(app, host=HOST, port=PORT)


if __name__ == '__main__':
    start()
