#!/usr/bin/env python

import asyncio
import json
import logging
import time
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

DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'handlers': {
        'console': {
            'level': "DEBUG",
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'level': "DEBUG",
            'handlers': ['console'],
            'propagate': False,
        },
    },
}

logging.config.dictConfig(DEFAULT_LOGGING)

logger = logging.getLogger('')

HOST = '0.0.0.0'
PORT = int(os.getenv('SOCKETSHARK_PORT', 9011))
redis_host = os.getenv('socketshark.pysocketshark.py', 'localhost')
redis_password = os.getenv('REDIS_PASSWORD', '')

redis_access = Redis(host=redis_host, password=redis_password)

routes = web.RouteTableDef()
sockets: typing.List['Socket'] = []

expire_queue = asyncio.Queue()


class Socket(typing.NamedTuple):
    user_id: int
    websocket: web.WebSocketResponse
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

    # chan = await redis_worker_access.subscribe("common.quotes")
    # quotes = chan[0]

    asyncio.create_task(reader_private(private, 'private.messages'))
    asyncio.create_task(reader_common(common, 'common.messages'))
    asyncio.create_task(reader_common(quotes, 'common.quotes'))
    # asyncio.create_task(redis_test(quotes))
    # asyncio.create_task(check_connections())


async def redis_test(common):
    logger.debug('check redis initialized')
    while True:
        message = await common.get_json()
        logger.info(json.dumps(message))


async def check_connections():
    logger.debug('check connections initialized')
    while True:
        for socket in sockets:
            if socket.websocket.closed:
                sockets.remove(socket)


async def reader_common(common, name):
    logger.debug(f' reader common for {name} initialized')
    while True:
        message = await common.get_json()
        # logger.info(json.dumps(message))
        logger.info(str(len(sockets)) + str(message))
        # if len(sockets) > 1:
        for socket in sockets:
            await socket.websocket.send_json(message)
            await asyncio.sleep(0.1)


async def reader_private(private, name):
    logger.debug(f'reader private for {name} initialized')
    while True:
        message = await private.get_json()
        # if len(sockets) > 1:
        logger.info(str(len(sockets)) + ": " + message)
        for socket in sockets:
            if message['user_id'] == socket.user_id and name in socket.subscriptions:
                await socket.websocket.send_json(message)
                await asyncio.sleep(0.1)


#
@routes.get(r'/{token:\w+}')
async def on_message(request: Request):
    websocket = web.WebSocketResponse(heartbeat=10)
    await websocket.prepare(request)

    logger.debug("New attempt!")
    token = request.match_info['token']
    user_id = int(redis_access.hget('private.tokens', token))
    if not token or not user_id:
        logger.debug(f"no token ({token}) or user!")
        return web.Response(text="Hello, friend. You'd better go away.")

    logger.debug("New websocket!")
    logger.debug('token:' + token + 'user_id:' + str(user_id))

    await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=30)))

    socket = Socket(user_id, websocket, set())

    sockets.append(socket)

    async for websocket_message in websocket:  # type: WSMessage
        if websocket_message.type == WSMsgType.TEXT:
            logger.debug("adding a subscription")
            data = json.loads(websocket_message.data)
            socket.subscriptions.add(data['subscribe'])
        elif websocket_message.type == WSMsgType.ERROR:
            logger.debug('ws connection closed with exception %s' %
                         websocket.exception())

    sockets.remove(socket)

    logger.debug('websocket connection closed')

    return websocket


async def create_expire_task(app):
    asyncio.create_task(expire_task())


async def expire_task():
    logger.debug('expire task initialized')
    while True:
        for _ in range(expire_queue.qsize()):
            token, user_id, time = await expire_queue.get()
            if time <= datetime.now():
                logger.debug(f"deleted token {token}")
                logger.debug(f"deleted user {user_id}")
                redis_access.hdel('private.tokens', token)
                redis_access.hdel('private.users', user_id)
            else:
                await expire_queue.put((token, user_id, time))
        await asyncio.sleep(0.1)


def start():
    app = web.Application()
    app.router.add_routes(routes)
    app.on_startup.append(redis_worker)
    app.on_startup.append(create_expire_task)

    logger.warning('Starting Socketshark!')
    logger.warning(f"started at {time.strftime('%X')}")
    web.run_app(app, host=HOST, port=PORT)


if __name__ == '__main__':
    start()
