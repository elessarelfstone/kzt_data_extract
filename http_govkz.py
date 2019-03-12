import luigi
import os
import re
from shutil import copyfile
import requests
from bs4 import BeautifulSoup
import pandas as pd

from urllib import parse
import settings
from utils import Utils
from rarfile import RarFile
import zipfile
from excel_parsing import ExcelParsing


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


class DownloadFiles(luigi.Task):

    file = luigi.Parameter()

    def source_file_path(self, suff):
        path = os.path.join(os.getenv('DATA_DIR'), 'http',"{}_{}.{}".format(Utils.params(self.file).name, suff, Utils.params(self.file).ext[0]))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def retrieve_urls_list(self):
        html = requests.get(Utils.params(self.file).urls_location).text
        soup = BeautifulSoup(html, 'lxml')
        urls = soup.find("div", attrs=Utils.params(self.file).html_container_attrs).find_all("a", href=re.compile(Utils.params(self.file).html_url_regexp))
        urls = [url.get('href') for url in urls]
        return urls

    def retrieve(self, url, suff):
        try:
            try:
                file_url = Utils.params(self.file).base_url + url
                result = requests.get(file_url, verify=False, stream=True)
                with open(self.source_file_path(suff), 'wb') as f:
                    f.write(result.content)
            except Exception as e:
                raise e
        finally:
            pass

    def output(self):
        return [luigi.LocalTarget(self.source_file_path(i)) for i, _ in enumerate(self.retrieve_urls_list())]

    def run(self):
        for i, url in enumerate(self.retrieve_urls_list()):
            self.retrieve(url, i)


class CollectExcelFileToCsv(luigi.Task):

    file = luigi.Parameter()

    def collect_from_xls(self):
        data = ExcelParsing.parse_file(self.file, self.input().path)
        data = data.iloc[:, 0:len(Utils.params(self.file).structure)]
        data.columns = Utils.params(self.file).structure
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

    def collect_from_xlss(self):
        all_data = pd.DataFrame()
        for f in self.input():
            data = ExcelParsing.parse_file(self.file, f.path)
            data = data.iloc[:, 0:len(Utils.params(self.file).structure)]
            data.columns = Utils.params(self.file).structure
            all_data = all_data.append(data, ignore_index=True)

        all_data.to_csv(self.output().path, sep=';', encoding='utf-8', header=None, index=None)

    def requires(self):
        return ExtractArchive(file=self.file)

    def output(self):
        folder = os.path.dirname(self.input()[0].path)
        file = "{}.csv".format(Utils.params(self.file).name)
        path = os.path.join(folder, file)
        return luigi.LocalTarget(path)

    def run(self):
        self.collect_from_xlss()

        # return [CollectExcelFileToCsv(file=self.file) for member in self.input()]

class ExtractArchive(luigi.Task):

    file = luigi.Parameter()

    def requires(self):
        return DownloadFile(file=self.file)

    def output(self):
        return [luigi.LocalTarget(os.path.join(os.getenv('DATA_DIR'), 'http', file)) for file in Utils.params(self.file).files]

    def run(self):
        ext = os.path.splitext(self.input().path)[-1]
        if ext == '.rar':
            arch = RarFile(self.input().path)
        elif ext == '.zip':
            arch = zipfile.ZipFile(self.input().path)

        for f in arch.namelist():
            if os.path.basename(f) in [os.path.basename(out.path) for out in self.output()]:
                # temp = os.path.join(os.getenv('TEMP_DIR'), f)
                target = os.path.join(os.getenv('DATA_DIR'), 'http', f)
                arch.extract(f, os.path.join(os.getenv('DATA_DIR'), 'http'))
                copyfile(target, os.path.join(os.getenv('DATA_DIR'), 'http', os.path.basename(f)))


class ExtractArchives(luigi.Task):

    file = luigi.Parameter()

    def requires(self):
        DownloadFiles(file=self.file)

    def output(self):
        [ExtractArchive(file=self) for ]


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


class HttpStatGovKATO(CollectArchiveToCsv):
    pass


class HttpKgdGovPseudoCompany(CollectExcelFileToCsv):
    pass


class HttpKgdGovWrongAddress(CollectExcelFileToCsv):
    pass


class HttpKgdGovBankrupt(CollectExcelFileToCsv):
    pass


class HttpKgdGovInactive(CollectExcelFileToCsv):
    pass


class HttpKgdGovInvalidRegistration(CollectExcelFileToCsv):
    pass


class HttpKgdGovViolationTaxCode(CollectExcelFileToCsv):
    pass


class HttpKgdGovTaxArrearsULOver150(CollectExcelFileToCsv):
    pass