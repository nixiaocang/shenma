import json
import asyncio
import logging
from _sem_service import ShenMaSemService
logger = logging.getLogger()


class AccountAuth(object):
    def __init__(self, request_params):
        self.f_account = request_params.get("account", None)
        self.f_password = request_params.get("password", None)
        self.f_token = request_params.get("token", None)

    async def auth_account(self):
        res_code = 0
        res_message = None

        try:
            sem_service = ShenMaSemService(self.f_account, self.f_password, self.f_token)
            request_body = {
                    "requestData":["account_all"]
                    }
            res = await sem_service.auth_account(request_body)
            res = json.loads(res)
            if res['header']['status'] != 0:
                res_code = 2100
                res_message = "Failed to auth with shenma server, user info is NOT matched"
            else:
                res_code = 2000
                res_message = "Succeed to auth with shenma server."
        except Exception as e:
            res_code = 2101
            res_message = ("Exception is thrown during authing: %s" % str(e))
        finally:
            logger.info(res_message)
            print(res_code, res_message)
            return {
                "status": res_code,
                "message": res_message
            }

