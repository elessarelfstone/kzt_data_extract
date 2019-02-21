import os
import json
from pathlib import Path
from datetime import datetime, timedelta
import luigi
from luigi.format import UTF8
from oradump import OraDump
import settings
import time


class OraTable(luigi.Task):

    file = luigi.Parameter()
    table = luigi.Parameter()

    def load_params(self, file):
        json_param = Path("params") / file
        with open(json_param, "r") as f:
            raw_params = f.read()
        return raw_params

    def output(self):
        raw_param = self.load_params(self.file)
        params = json.loads(raw_param)
        # print(params)
        # директория для всех данных
        data_root = os.getenv("DATA_DIR")
        # date = datetime.strptime(params["date"], "%d.%m.%Y")
        date = datetime.today() - timedelta(days=1)
        # директория для файла
        data_dir = Path(data_root) / params["code"] / "{date:%Y/%m/%d}".format(date=date)
        # путь для файла
        file_path = str(data_dir / "{}-{}-{}.csv.gzip".format(params["code"], self.table, date.strftime("%Y%m%d")))
        return luigi.LocalTarget(file_path, format=UTF8)

    def run(self):
            raw_param = self.load_params(self.file)
            params = json.loads(raw_param)
            # params.update(table=self.table)
            date = datetime.today() - timedelta(days=1)
            params.update(date=date.strftime("%d.%m.%Y"))
            conn = "{}/{}@{}".format(params["user"], params["pass"], params["tns"])
            source_type_code = params["code"].split("_")[0]
            sql_template_path = Path("templates") / source_type_code / "{}_{}.sqtmpl".format(source_type_code, self.table)
            cnt = OraDump.dump_gziped(conn, sql_template_path.read_text(), Path(self.output().path), params, True)
