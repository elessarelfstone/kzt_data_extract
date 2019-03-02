import luigi
import os
from shutil import copyfile
import requests
import pandas as pd
from urllib import parse
import settings
from utils import Utils


class DownloadFile(luigi.Task):
    file = luigi.Parameter()

    # def params(self):
    #     return Utils.params_named_tuple(Utils.params_file_path(self.file))

    def source_file_path(self):
        path = os.path.join(os.getenv('DATA_DIR'), 'http', "{}.{}".format(Utils.params(self.file).name, self.params(self.file).ext))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def retrieve(self):
        try:
            try:
                result = requests.get(self.params().url, verify=False, stream=True)
                with open(self.source_file_path(), 'wb') as f:
                    f.write(result.content)
            except Exception as e:
                raise e
        finally:
            pass

    def output(self):
        return luigi.LocalTarget(self.source_file_path())

    def run(self):
        self.retrieve()


class CollectExcelFileToCsv(luigi.Task):

    file = luigi.Parameter()


    def collect_from_xls(self):
        data = pd.DataFrame()
        xls = pd.ExcelFile(self.input().path)
        xls_sheets = xls.sheet_names
        if Utils.params(self.file).sheet is None:
            for sheet in xls_sheets:
                df = pd.read_excel(self.input().path, sheet_name=sheet, skiprows=Utils.params(self.file)., index_col=None, dtype=str, header=None)

        df.to_csv(self.output().path, sep=';', encoding='utf-8')

    def requires(self):
        return DownloadFile(file=self.file)

    def output(self):
        folder = os.path.dirname(self.input().path)
        file = "{}.csv".format(os.path.splitext(os.path.basename(self.input().path))[0])
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        # copyfile(self.input().path, self.output().path)
        self.collect_from_xls()
