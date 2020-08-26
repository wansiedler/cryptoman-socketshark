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

LOGLEVEL = os.environ.get('LOGLEVEL', 'debug').upper()

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
    },
    'loggers': {
        'connectors.socketshark': {
            'level': LOGLEVEL,
            'handlers': ['console'],
            'propagate': True,
        },
    },
}

logging.config.dictConfig(DEFAULT_LOGGING)

logger = logging.getLogger('connectors.socketshark')

redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_password = os.getenv('REDIS_PASSWORD', '')

logger.debug(redis_host)
if redis_password:
    r: Redis = redis.client.StrictRedis(host=redis_host, password=redis_password)
else:
    r: Redis = redis.client.StrictRedis(host=redis_host)
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
        await expire_queue.put((token, user_id, datetime.now() + timedelta(minutes=5)))
        row = SocketRow(user_id, ws, set())
        sockets.append(row)
    else:
        return ws

    async for msg in ws:  # type: WSMessage
        if msg.type == WSMsgType.TEXT:
            data = json.loads(msg.data)
            row.subscriptions.add(data['subscribe'])
        elif msg.type == WSMsgType.ERROR:
            logger.debug('ws connection closed with exception %s' %
                         ws.exception())

    sockets.remove(row)

    logger.debug('websocket connection closed')

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
    local_r = await aioredis.create_redis('redis://'+redis_host)
    chan = await local_r.subscribe("common.messages")
    common = chan[0]
    chan = await local_r.subscribe("private.messages")
    private = chan[0]
    chan = await local_r.subscribe("common.quotes")
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
