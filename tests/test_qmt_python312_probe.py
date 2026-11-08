import ast
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


class ProbeTests(unittest.TestCase):
    def test_ascii_python36_source_and_readonly_execution(self):
        path = Path(__file__).resolve().parents[1] / 'scripts/qmt_python312_probe_qmt.py'
        source = path.read_bytes().decode('ascii')
        ast.parse(source, feature_version=(3, 6))
        spec = importlib.util.spec_from_file_location('probe', path)
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        calls = []
        def query(account, account_type, kind):
            calls.append(kind)
            if kind == 'ORDER':
                raise ValueError('broker error for ' + account)
            return [SimpleNamespace(m_strAccountID=account, m_strAccountKey='secret',
                                    m_strRemark='private-note', m_dComssion=0.44352,
                                    m_dCommission=0.44352, m_dMissing=float('nan'))]
        def forbidden(*args, **kwargs):
            self.fail('Unexpected broker mutation')
        probe.get_trade_detail_data = query
        probe.passorder = forbidden
        probe.cancel = forbidden
        context = SimpleNamespace(set_account=forbidden, get_full_tick=lambda codes: {})
        with tempfile.TemporaryDirectory() as temp, redirect_stdout(io.StringIO()):
            probe.OUTPUT_DIR = temp
            probe.ACCOUNT_ID = 'PRIVATE-ACCOUNT'
            probe.HISTORY_START = probe.HISTORY_END = '20261106'
            probe.init(context)
            probe.after_init(context)
            probe.handlebar(context)
            probe.stop(context)
            raw = Path(probe._REPORT_PATH).read_text()
        self.assertEqual(calls, ['ACCOUNT', 'POSITION', 'ORDER', 'DEAL'])
        self.assertNotIn('PRIVATE-ACCOUNT', raw)
        self.assertNotIn('private-note', raw)
        self.assertNotIn('secret', raw)
        records = {record['name']: record for record in map(json.loads, raw.splitlines())}
        self.assertEqual(records['query.ORDER']['error_type'], 'ValueError')
        self.assertEqual(records['history.ORDER']['status'], 'MISSING')
        self.assertEqual(records['quote.full_tick']['result']['count'], 0)
        fields = records['query.DEAL']['result']['samples'][0]['fields']
        self.assertEqual(fields['m_dCommission']['value'], 0.44352)
        self.assertEqual(fields['m_dMissing']['value'], {'nonfinite': 'nan'})


if __name__ == '__main__':
    unittest.main()
