import json
import asyncio
import logging
from io import StringIO
from tornado.httpclient import AsyncHTTPClient
import pandas as pd
import tornado.gen
logger = logging.getLogger()


class ReportBase(object):
    @staticmethod
    async def get_report_data(sem_service, request_body):
        logger.info("########### Ready to get report data ###########")

        data_info = {}
        logger.info("########### Start to get_report_id ###########")

        report_id_res = await sem_service.get_report_id(request_body)
        report_id_res = json.loads(report_id_res)
        if report_id_res['header']['status'] != 0:
            raise Exception("获取report_id失败：%s" % report_id_res['header']['failures'][0]['message'])

        logger.info("########### Finished to get_report_id, result: %s ###########"
                         % json.dumps(report_id_res))


        report_id = report_id_res['body']['taskId']
        report_param = {'taskId': report_id}
        logger.info("report_id is : %s" % report_id)

        retry = 0
        retry_times = 3
        retry_interval = 5 #seconds
        while retry < retry_times:
            report_state_res = await sem_service.get_report_state(report_param)
            report_state_res = json.loads(report_state_res)
            if report_state_res['header']['status'] != 0:
                retry += 1
                if retry == retry_times:
                    logger.info("retry times is up to max for get_report_state.")
                    raise Exception("获取report_state失败,report_state_res['header']['status']:%s" %
                                        report_state_res['header']['status'])

                logger.info("get_report_state.header.status is NOT equal to 0, now retry again. "
                                 "retry times: %s error : %s" %
                                 (retry, report_state_res['header']['failures'][0]['message']))
                await tornado.gen.sleep(retry_interval)
            else:
                report_generated_status = report_state_res['body']['status']
                if report_generated_status != "FINISHED":
                    retry += 1
                    if retry == retry_times:
                        logging.info("retry times is up to max for report_state_res.status.")
                        raise Exception("获取报告失败,report_generated_status:%s" % report_generated_status)

                    logger.info('report is NOT generated : retry times: %s，report_id: %s' %
                                     (retry, report_id))
                    await tornado.gen.sleep(retry_interval)
                else:
                    logger.info("report state is FINISHED")
                    break

        logger.info("########### Finished to get_report_state, result: %s ###########" %
                         json.dumps(report_state_res))

        logger.info("Start to get_report_file_data")

        report_param = {'fileId': report_id}
        report_file_res = await sem_service.get_report_file(report_param)
        if not report_file_res:
            raise Exception("获取report_file失败，report_file_res :%s"
                                % report_file_res)

        logger.info("########### Finished to get_report_file")
        file_name = 'csv/%s.csv' % report_id
        report_file_res  = str(report_file_res, encoding = "utf-8")
        with open(file_name, 'w') as f:
            f.write(report_file_res)
        _report_data = pd.read_csv(file_name, sep=',')
        logger.info("########### Finished to write report file and convert data ###########")
        return {
            'length': len(_report_data),
            'report_data': _report_data,
            'retry_times': retry
        }

    @staticmethod
    def convert_sem_data_to_pt(fres, f_source, f_company_id, f_email, fmap, f_account):
        '''
            注意：
            此处只是数据源的原始字段转换为ptming的入库字段，不做字段特殊值处理
            (比如--，null等转换为'')，每个接口的字段特殊处理在每个业务接口中单独处理

        '''
        fres['f_source'] = f_source
        fres['f_company_id'] = f_company_id
        fres['f_email'] = f_email
        fres['f_account'] = f_account

        cols = [col for col in fres]
        new_cols = []
        for col in cols:
            if col not in fmap.keys():
                del fres[col]
            else:
                new_cols.append(fmap[col])
        fres.columns = new_cols
        return fres
