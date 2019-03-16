import os
import json
from pathlib import Path
from datetime import datetime, timedelta
import luigi
from luigi.format import UTF8
from oradump import OraDump
from utils import Utils
import settings
import time


class OraTable(luigi.Task):

    file = luigi.Parameter()
    table = luigi.Parameter()

    def load_params(self, file):
        json_param = Path("params") / 'http' /file
        # json_param = os.path.join('params', 'http', file)
        with open(json_param, "r") as f:
            raw_params = f.read()
        return raw_params

    def output(self):
        # директория для всех данных
        data_root = os.getenv("DATA_DIR")
        # date = datetime.strptime(params["date"], "%d.%m.%Y")
        date = datetime.today() - timedelta(days=1)
        # директория для файла
        data_dir = os.path.join(data_root, Utils.params(self.file).code, "{date:%Y/%m/%d}".format(date=date))
        # путь для файла
        file_path = os.path.join(data_dir, "{}-{}-{}.csv.gzip".format(Utils.params(self.file).code, self.table, date.strftime("%Y%m%d")))
        return luigi.LocalTarget(file_path, format=UTF8)

    def run(self):
        file_path = Utils.params_file_path(self.file)
        params = json.loads(Utils.read_text(file_path))
        date = datetime.today() - timedelta(days=1)
        params.update(date=date.strftime("%d.%m.%Y"))
        conn = "{}/{}@{}".format(Utils.params(self.file).user, Utils.params(self.file).password,  Utils.params(self.file).tns)
        src_type_code = Utils.params(self.file).code.split("_")[0]
        template_path = os.path.join('templates', src_type_code, "{}_{}.sqtmpl".format(src_type_code, self.table))

        cnt = OraDump.dump_gziped(conn, Utils.read_text(template_path), Path(self.output().path), params, True)
