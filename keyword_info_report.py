import asyncio
import logging
import logging.config
import time
import json
import datetime
import pandas as pd
from _sem_service import ShenMaSemService
from _report_base import ReportBase
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger()


'''
    每个接口中需要转换的字段映射表，都统一放在单独的业务接口中
'''
fmap = {
        "f_source": "f_source",
        "f_company_id": "f_company_id",
        "f_email": "f_email",
        "date": "f_date",
        "账户": "f_account",
        "推广计划": "f_campaign",
        "推广单元": "f_set",
        "设备": "f_device",
        "keywordId": "f_keyword_id",
        "账户ID": "f_account_id",
        "adgroupId": "f_set_id",
        "keyword": "f_keyword",
        "price": "f_keyword_offer_price",
        "destinationUrl": "f_mobile_url",
        "matchType": "f_matched_type",
        "quality": "f_keyword_mobile_quality"
    }


class KeywordInfoReport(ReportBase):
    def __init__(self, request_params):
        self.f_account = request_params.get("account", None)
        self.f_password = request_params.get("password", None)
        self.f_token = request_params.get("token", None)
        self.f_email = request_params.get("pt_email", None)
        self.f_company_id = request_params.get("pt_company_id", None)
        self.f_source = request_params.get("pt_source", None)
        self.f_from_date = request_params.get("pt_data_from_date", None)
        self.f_to_date = request_params.get("pt_data_to_date", None)


    async def get_data(self):
        res_code = 0
        res_message = None

        try:
            if self.f_account is None \
                    or self.f_password is None \
                    or self.f_email is None \
                    or self.f_company_id is None \
                    or self.f_source is None \
                    or self.f_token is None:
                res_code = 2100
                res_message = "Failed to get_report_data, account or password or token is  missed"
            else:
                sem_service = ShenMaSemService(self.f_account, self.f_password, self.f_token)

                if self.f_from_date is None or self.f_to_date is None:
                    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
                    start_date = yesterday
                    end_date = yesterday
                else:
                    start_date = self.f_from_date
                    end_date = self.f_to_date
                request_body = {
                        "performanceData": ["cost", "cpc", "click", "impression",     "ctr", "rank"],
                        "startDate": start_date,
                        "endDate": end_date,
                        "idOnly": False,
                        "reportType": 14,
                        "format": 2,
                        "unitOfTime": 5
                        }
                res_code, res_message = await self._get_and_save_report_data_to_db(sem_service, request_body)
        except Exception as e:
            res_code = 2101
            res_message = ("Exception is thrown during get_report_data : %s" % str(e))
        finally:
            logger.info(res_message)
            print(res_code, res_message)
            return {
                "status": res_code,
                "message": res_message
            }

    async def _get_and_save_report_data_to_db(self, sem_service, request_body):
        '''
            开始获取数据计时
        '''
        start = time.time()
        get_report_data_res = await ReportBase.get_report_data(sem_service, request_body)
        df = get_report_data_res.get("report_data", None)
        dates = pd.date_range(self.f_from_date, self.f_to_date)
        data = []
        for date in dates:
            date = str(date)[:10]
            tdf = df[df['时间']==date]
            ids = tdf["关键词ID"].tolist()
            ids = list(set(ids))
            for i in range(0, len(ids), 5000):
                request_body = {
                        "keywordIds":ids[i:i+5000]
                    }
                res = await sem_service.get_keyword(request_body)
                res = json.loads(res)
                if res["header"]["status"] != 0:
                    raise Exception("获取keyword info失败, return :%s" % json.dumps(res))
                sub_data = res["body"]["keywordTypes"]
                request_body = {
                        "ids":ids[i:i+5000],
                        "type":7
                        }
                qres = await sem_service.get_keyword_quality(request_body)
                qres = json.loads(qres)
                if qres["header"]["status"] != 0:
                    raise Exception("获取keyword info失败, return :%s" % json.dumps(qres))
                sub_qualities = qres["body"]["keyword10Quality"]
                qbag = {}
                for item in sub_qualities:
                    qbag[item["id"]] = item["quality"]
                for sub in sub_data:
                    sub["date"] = date
                    sub["quality"] = qbag.get(sub["keywordId"], 0)
                data += sub_data
        cost = time.time()-start
        print("keyword info耗时==> %s s" % cost)
        '''
            计算获取数据用时
        '''

        '''

            1. 如过该接口返回的数据中包含特殊值，比如--, null, 空，请在此处转换成接口文档中的默认值
            2. 清洗完数据之后，到此返回数据即可，数据可以缓存在csv文件中。
        '''
        report_data = pd.read_json(json.dumps(data))
        tdf = df[['推广计划ID','关键词ID', '账户ID', '推广计划', '推广单元', '账户']]
        tdf.rename(columns={'关键词ID':'keywordId'}, inplace = True)
        report_data = pd.merge(report_data, tdf, on='keywordId')
        fres = ReportBase.convert_sem_data_to_pt(report_data, self.f_source, self.f_company_id, self.f_email, fmap, self.f_account)
        fres.to_csv("csv/keyword_info_report.csv")
        return 2000, "OK"
