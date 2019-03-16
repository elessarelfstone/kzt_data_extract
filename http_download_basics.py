import luigi
import os
import re
import requests
from bs4 import BeautifulSoup
import settings
from utils import Utils


class DownloadFile(luigi.Task):
    """
    The task downloads one file. A file is being downloaded to TEMP_DIR which specified in ENV

    Attributes
    ----------
    file : str
        the file that stores all parameters for the task
    """
    file = luigi.Parameter()

    def file_path(self):
        """
        Gets path for target file
        :return: string that represents full path to target file
        """
        path = os.path.join(os.getenv('TEMP_DIR'), "{}.{}".format(Utils.params(self.file).name, Utils.params(self.file).ext))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def download(self):
        try:
            try:
                result = requests.get(Utils.params(self.file).url, verify=False, stream=True)
                with open(self.output().path, 'wb') as f:
                    f.write(result.content)
            except Exception as e:
                if os.path.exists(self.output().path):
                    os.remove(self.output().path)
                raise e
        finally:
            pass

    def output(self):
        return luigi.LocalTarget(self.file_path())

    def run(self):
        self.download()


class DownloadFiles(luigi.Task):
    """
    The task downloads a bunch of files that represented by list of urls. A file is being downloaded to TEMP_DIR which specified in ENV

    Attributes
    ----------
    file : str
        the file that stores all parameters for the task
    """
    file = luigi.Parameter()

    def file_path(self, suffix):
        """
        Gets path for target file
        :param suffix:
        :return: string that represents full path to target file
        """
        dir = os.path.join(os.getenv('TEMP_DIR'))
        file = "{}_{}.{}".format(Utils.params(self.file).name, suffix, Utils.params(self.file).ext[0])
        path = os.path.join(dir, file)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def urls_list(self):
        """
        Gets list of urls
        :return:
        """
        html = requests.get(Utils.params(self.file).urls_location).text
        soup = BeautifulSoup(html, 'lxml')
        container = Utils.params(self.file).html_container_tag
        attrs = Utils.params(self.file).html_container_attrs
        href_regex = Utils.params(self.file).html_url_regexp
        urls = soup.find(container, attrs=attrs).find_all("a", href=re.compile(href_regex))
        urls = [url.get('href') for url in urls]
        return urls

    def download(self, url, suffix):
        try:
            try:
                file_url = Utils.params(self.file).base_url + url
                result = requests.get(file_url, verify=False, stream=True)
                with open(self.file_path(suffix), 'wb') as f:
                    f.write(result.content)
            except Exception as e:
                if os.path.exists(self.file_path(suffix)):
                    os.remove(self.file_path(suffix))
                raise e
        finally:
            pass

    def output(self):
        return [luigi.LocalTarget(self.file_path(i)) for i, _ in enumerate(self.urls_list())]

    def run(self):
        for i, url in enumerate(self.urls_list()):
            self.download(url, i)

