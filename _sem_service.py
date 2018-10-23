import json
import urllib
import logging
import asyncio
import hashlib
import binascii
import tornado.gen
from tornado.httpclient import AsyncHTTPClient

logger = logging.getLogger()

class ShenMaSemService(object):
    def __init__(self, username=None, password=None, token=None):
        self.username = username
        self.password = password
        self.token = token

    async def __execute(self, service, method, request_body):
        url = "https://e.sm.cn/api/" + service + "/" + method
        headers = {"content-type":"application/json"}
        data = {
                "header": {
                    "username": self.username,
                    "password": self.password,
                    "token": self.token
                                            },
                "body": request_body
                }
        data = json.dumps(data)
        logger.info("Request body shenma: %s" % data)

        client = AsyncHTTPClient()
        r = await client.fetch(url, method="POST", headers=headers, body=data)
        return r.body

    async def auth_account(self, request_body=None):
        return await self.__execute(service="account", method="getAccount", request_body=request_body)

    async def get_report_id(self, request_body=None):
        return await self.__execute(service="report", method="getReport", request_body=request_body)

    async def get_report_state(self, request_body=None):
        return await self.__execute(service="task", method="getTaskState", request_body=request_body)

    async def get_report_file(self, request_body=None):
        return await self.__execute(service="file", method="download", request_body=request_body)

    async def get_keyword(self, request_body=None):
        return await self.__execute(service="keyword", method="getKeywordByKeywordId", request_body=request_body)

