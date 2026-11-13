import json
import threading
import unittest
from urllib.request import urlopen
from src import source

class SupplierSourceTests(unittest.TestCase):
    def setUp(self): self.source = source.Source()
    def route(self, path, headers=None): return self.source.route('GET', path, headers or {}, b'')
    def test_protocol(self):
        total = self.route('/search?start=10&end=11')[2]
        value = json.loads(total)
        self.assertEqual((value['total'], len(value['records'])), (3, 2))
        self.assertEqual(len(json.loads(self.route('/search?start=10&end=11&provider=甲')[2])['records']), 2)
        self.assertEqual(json.loads(self.route('/search?start=11&end=12')[2])['records'][0]['id'], 'd')
    def test_http_state_endpoint(self):
        with source.make_server() as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                with urlopen('http://127.0.0.1:%d/__state' % server.server_port, timeout=5) as response:
                    self.assertEqual(json.load(response)['generation'], 1)
            finally:
                server.shutdown(); thread.join()

if __name__ == '__main__': unittest.main()
