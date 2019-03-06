import luigi
import os
from shutil import copyfile
import requests
import pandas as pd
from urllib import parse
import settings
from utils import Utils
import rarfile
import zipfile


class DownloadFile(luigi.Task):
    file = luigi.Parameter()

    def source_file_path(self):
        path = os.path.join(os.getenv('DATA_DIR'), 'http', "{}.{}".format(Utils.params(self.file).name, Utils.params(self.file).ext))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def retrieve(self):
        try:
            try:
                result = requests.get(Utils.params(self.file).url, verify=False, stream=True)
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
        if len(Utils.params(self.file).sheets) == 0:
            for sheet in xls_sheets:
                df = pd.read_excel(self.input().path,
                                   sheet_name=sheet,
                                   skiprows=Utils.params(self.file).skiprows,
                                   index_col=None,
                                   dtype=str,
                                   header=None)
                data = data.append(df, ignore_index=True)
        # data = data.iloc[1:, 0:len(Utils.params(self.file).structure)]
        else:
            for i in Utils.params(self.file).sheets:
                df = pd.read_excel(self.input().path,
                                   sheet_name=xls_sheets[i],
                                   skiprows=Utils.params(self.file).skiprows,
                                   index_col=None,
                                   dtype=str,
                                   header=None)
                data = data.append(df, ignore_index=True)

        data.columns = Utils.params(self.file).structure
        data = data.replace(['nan', 'None'], '', regex=True)
        data = data.dropna()
        # data = data.loc[:data[(data[Utils.params(self.file).index] == '')].index[0] - 1, :]
        data.to_csv(self.output().path, sep=';', encoding='utf-8', header=None, index=None)

    def requires(self):
        return DownloadFile(file=self.file)

    def output(self):
        folder = os.path.dirname(self.input().path)
        file = "{}.csv".format(os.path.splitext(os.path.basename(self.input().path))[0])
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        self.collect_from_xls()


class CollectArchiveToCsv(luigi.Task):

    file = luigi.Parameter()

    def requires(self):
        return ExtractArchive(file=self.file)

    def output(self):
        folder = os.path.dirname(self.input()[0].path)
        file = "{}.csv".format(Utils.params(self.file).name)
        return luigi.LocalTarget()

class ExtractArchive(luigi.Task):

    file = luigi.Parameter()

    def requires(self):
        return DownloadFile(file=self.file)

    def output(self):
        return [luigi.LocalTarget(os.path.join(os.getenv('DATA_DIR'), 'http', file)) for file in Utils.params(self.file).files]

    def run(self):
        ext = os.path.splitext(self.input().path)[-1]
        if ext == '.rar':
            arch = rarfile.RarFile(self.input().path)
        elif ext == '.zip':
            arch = zipfile.ZipFile(self.input().path)

        for f in arch.namelist():
            if f in [os.path.basename(out.path) for out in self.output()]:
                arch.extract(f, os.path.join(os.getenv('DATA_DIR'), 'http'))






class HttpStatGovOKED(CollectExcelFileToCsv):
    pass


class HttpStatGovKPVED(CollectExcelFileToCsv):
    pass


class HttpStatGovNVED(CollectExcelFileToCsv):
    pass


class HttpStatGovKURK(CollectExcelFileToCsv):
    pass


class HttpStatGovMKEIS(CollectExcelFileToCsv):
    pass
