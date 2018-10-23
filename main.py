import asyncio
from auth_account import AccountAuth
from campaign_report import CampaignReport
from creative_report import CreativeReport
from keyword_info_report import KeywordInfoReport
from keyword_report import KeywordReport
from region_report import RegionReport
from search_report import SearchReport

if __name__ == "__main__":
    request_params = {
            "account":"Wiseway-sm",
            "password":"WJ@zx902",
            "pt_email":"1234@qq.com",
            "pt_company_id":"1234abcd",
            "pt_source":"e360",
            "pt_data_from_date":"2018-10-19",
            "pt_data_to_date":"2018-10-20",
            "token":"c152ac99-4323-4650-8326-4acde308399c"
            }
    loop = asyncio.get_event_loop()
    objs = [CampaignReport,CreativeReport,KeywordInfoReport,KeywordReport,RegionReport,SearchReport]
    tasks = [asyncio.ensure_future(AccountAuth(request_params).auth_account())] + [asyncio.ensure_future(obj(request_params).get_data()) for obj in objs]
    loop.run_until_complete(asyncio.wait(tasks))
    print("all report get finish")
