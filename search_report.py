import asyncio
import logging
import logging.config
import time
import datetime
from _sem_service import ShenMaSemService
from _report_base import ReportBase
import traceback

logger = logging.getLogger()


'''
    每个接口中需要转换的字段映射表，都统一放在单独的业务接口中
'''
fmap = {
        "f_source": "f_source",
        "f_company_id": "f_company_id",
        "f_email": "f_email",
        "时间": "f_date",
        "账户": "f_account",
        "账户ID": "f_account_id",
        "推广计划": "f_campaign",
        "计划ID": "f_campaign_id",
        "推广单元": "f_set",
        "单元ID": "f_set_id",
        "关键词": "f_keyword",
        "关键词ID": "f_keyword_id",
        "搜索词": "f_search_word",
        "创意ID": "f_creative_id",
        "创意": "f_creative_title",
        "展现量": "f_impression_count",
        "点击量": "f_click_count",
        "消费": "f_cost",
        "匹配方式": "f_matched_type",
        "当前单元添加状态": "f_set_state"
    }


class SearchReport(ReportBase):
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
                    or self.f_token is None \
                    or self.f_email is None \
                    or self.f_company_id is None \
                    or self.f_source is None:
                res_code = 2100
                res_message = "Failed to get_report_data, account or password or token is missed"
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
                        "performanceData": ["click", "impression", "cost"],
                        "startDate": start_date,
                        "endDate": end_date,
                        "idOnly": False,
                        "reportType": 6,
                        "format": 2,
                        "unitOfTime": 5
                                    }
                res_code, res_message = await self._get_and_save_report_data_to_db(sem_service, request_body)
        except Exception as e:
            traceback.print_exc()
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
        cost = time.time()-start
        print("search report耗时==> %s s" % cost)
        '''
            计算获取数据用时
        '''
        report_data_length = get_report_data_res.get("length", None)
        report_data = get_report_data_res.get("report_data", None)
        retry_times = get_report_data_res.get("retry_times", None)

        '''

            1. 如过该接口返回的数据中包含特殊值，比如--, null, 空，请在此处转换成接口文档中的默认值
            2. 清洗完数据之后，到此返回数据即可，数据可以缓存在csv文件中。
        '''
        fres = ReportBase.convert_sem_data_to_pt(report_data, self.f_source, self.f_company_id, self.f_email, fmap, self.f_account)
        fres.to_csv("csv/search.csv")
        return 2000, "OK"
