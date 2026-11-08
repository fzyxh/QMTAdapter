import asyncio
import csv
import os
from pathlib import Path
import unittest

from qmt_adapter import AsyncQmtClient, QmtClient, RemoteError
from tests import test_named_pipe_e2e as fixture


@unittest.skipUnless(os.name == 'nt', 'Windows named pipes are required')
class AccountFlowTests(unittest.TestCase):
    setUp = fixture.NamedPipeEndToEndTests.setUp
    tearDown = fixture.NamedPipeEndToEndTests.tearDown

    def directory(self):
        path = Path(self.temp_dir.name) / 'automatic-export'
        path.mkdir()
        self.harness.runtime.config['flow_export_dir'] = str(path)
        return path

    def test_newest_snapshot_is_discovered_filtered_and_paged_without_a_file_argument(self):
        directory = self.directory()
        account = fixture.ACCOUNT_ID
        fields = ['资金账号', '发生日期', '发生时间', '流水序号', '业务名称',
                  '发生金额', '剩余金额', '发生数量', '剩余数量', '证券代码']
        records = [
            [account, '20261105', '10:00:00', '0001', '转入', '123.45', '123.45', '', '', ''],
            ['OTHER', '20261106', '10:00:00', '0002', '其他账号', '3', '3', '0', '0', ''],
            [account, '20261106', '10:01:00', '0003', '成交', '-1,234.560', '100.00', '100.00', '100', '000001'],
            [account, '20261106', '10:02:00', '0004', '股份调整', '0', '100.00', '-50', '50', '000001'],
        ]
        old = directory / (account + '_stkFundFlow.csv')
        old.write_text('older snapshot', encoding='utf-8')
        os.utime(old, (946684800, 946684800))
        latest = directory / (account + '_stkFundFlow_20261106.csv')
        with latest.open('w', encoding='gb18030', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(fields)
            writer.writerows(records)
        with QmtClient(config_path=self.config_path) as client:
            page = client.list_account_flows(account, start_date='2026-11-06', end_date='2026-11-06', limit=1)
            self.assertEqual(page['source'], 'QMT_AUTO_EXPORT')
            self.assertEqual((page['count'], page['total'], page['next_offset']), (1, 2, 1))
            self.assertEqual(page['first_record_date'], '2026-11-05')
            item = page['items'][0]
            self.assertEqual((item['flow_id'], item['security_code'], item['amount_change']),
                             ('0003', '000001', '-1234.560'))
            self.assertNotIn('raw', item)
            page = client.list_account_flows(account, start_date='2026-11-06', offset=1, include_raw=True)
            self.assertIsNone(page['next_offset'])
            self.assertEqual(page['items'][0]['quantity_change'], '-50')
            self.assertEqual(page['items'][0]['raw']['剩余数量'], '50')
            # A subsequent automatic output can have a different header and encoding.
            new = directory / (account + '_stkFundFlow_20261107.txt')
            new.write_text('资金账号\t发生日期\t流水序号\t业务名称\t发生金额\n'
                           + account + '\t20261107\t0005\t结息\t0.004\n', encoding='utf-16')
            os.utime(latest, (946684801, 946684801))
            result = client.list_account_flows(account)
            self.assertEqual(result['total'], 1)
            self.assertNotIn('quantity_change', result['available_fields'])
            self.assertIsNone(result['items'][0]['quantity_change'])
            self.assertEqual(result['items'][0]['amount_change'], '0.004')

    def test_source_errors_and_whitelist_are_distinct_from_empty_results(self):
        directory = self.directory()
        account = fixture.ACCOUNT_ID
        with QmtClient(config_path=self.config_path) as client:
            for who, code in ((account, 'FLOW_EXPORT_UNAVAILABLE'), ('OTHER', 'ACCOUNT_NOT_ALLOWED')):
                with self.assertRaises(RemoteError) as error:
                    client.list_account_flows(who)
                self.assertEqual(error.exception.code, code)
            path = directory / (account + '_stkFundFlow.csv')
            path.write_text('证券代码,余额\n', encoding='utf-8-sig')
            with self.assertRaises(RemoteError) as error:
                client.list_account_flows(account)
            self.assertEqual(error.exception.code, 'FLOW_EXPORT_FORMAT_ERROR')
            path.write_text('资金账号,发生日期,业务名称,发生金额\n', encoding='utf-8-sig')
            self.assertEqual(client.list_account_flows(account)['count'], 0)
            with self.assertRaises(RemoteError) as error:
                client.list_account_flows(account, limit=-1)
            self.assertEqual(error.exception.code, 'INVALID_ARGUMENT')

    def test_shared_stock_export_with_date_suffix_is_filtered_by_account(self):
        stock = self.directory() / 'Stock'
        stock.mkdir()
        path = stock / '16772-20261107.csv'
        path.write_text('资金账号,发生日期,业务名称,发生金额\n'
                        'OTHER,20261107,结息,111.00\n'
                        + fixture.ACCOUNT_ID + ',20261107,结息,0.07\n', encoding='gb18030')

        async def query():
            async with AsyncQmtClient(config_path=self.config_path) as client:
                return await client.list_account_flows(fixture.ACCOUNT_ID)

        result = asyncio.run(query())
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['items'][0]['amount_change'], '0.07')
