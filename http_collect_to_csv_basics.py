import os
import luigi
import pandas as pd
from utils import Utils
from excel_parsing import ExcelParsing
import http_download_basics as hdb
import http_extract_basics as heb
import settings


class CollectExcelFileToCsv(luigi.Task):

    file = luigi.Parameter()

    def collect_from_xls(self):
        data = ExcelParsing.parse_file(self.file, self.input().path)
        data = data.iloc[:, 0:len(Utils.params(self.file).structure)]
        data.columns = Utils.params(self.file).structure
        data.to_csv(self.output().path, sep=';', encoding='utf-8', header=None, index=None)

    def requires(self):
        return hdb.DownloadFile(file=self.file)

    def output(self):

        # folder = os.path.dirname(self.input().path)
        folder = os.path.join(os.getenv("DATA_DIR"), os.getenv("DATA_HTTP_SUBFOLDER"))
        file = "{}.csv".format(os.path.splitext(os.path.basename(self.input().path))[0])
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        self.collect_from_xls()


class CollectArchiveToCsv(luigi.Task):

    file = luigi.Parameter()

    def collect_from_xlss(self):
        all_data = pd.DataFrame()
        for f in self.input():
            data = ExcelParsing.parse_file(self.file, f.path)
            data = data.iloc[:, 0:len(Utils.params(self.file).structure)]
            data.columns = Utils.params(self.file).structure
            all_data = all_data.append(data, ignore_index=True)

        all_data.to_csv(self.output().path, sep=';', encoding='utf-8', header=None, index=None)

    def requires(self):
        return heb.ExtractArchive(file=self.file)

    def output(self):
        # folder = os.path.dirname(self.input()[0].path)
        folder = os.path.join(os.getenv("DATA_DIR"), os.getenv("DATA_HTTP_SUBFOLDER"))
        file = "{}.csv".format(Utils.params(self.file).name)
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        self.collect_from_xlss()


class CollectArchivesToCsv(luigi.Task):

    file = luigi.Parameter()

    def collect_from_xlss(self):
        xl_files_path = [file.path for file in self.input()]
        data = ExcelParsing.parse_files(self.file, xl_files_path)
        data = data.iloc[:, 0:len(Utils.params(self.file).structure)]
        data.columns = Utils.params(self.file).structure
        data.to_csv(self.output().path, sep=';', encoding='utf-8', header=None, index=None)

    def requires(self):
        return heb.ExtractArchives(file=self.file)

    def output(self):
        # folder = os.path.dirname(self.input()[0].path)
        folder = os.path.join(os.getenv("DATA_DIR"), os.getenv("DATA_HTTP_SUBFOLDER"))
        file = "{}.csv".format(Utils.params(self.file).name)
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        self.collect_from_xlss()