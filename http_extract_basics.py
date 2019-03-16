import luigi
import os
from shutil import copyfile
from rarfile import RarFile
import zipfile
from utils import Utils
import http_download_basics as hdb
import settings


class ExtractArchive(luigi.Task):
    """
        The task extracts one file. A file is being extracted to TEMP_DIR which specified in ENV

        Attributes
        ----------
        file : str
            the file that stores all parameters for the task
        """
    file = luigi.Parameter()

    def requires(self):
        return hdb.DownloadFile(file=self.file)

    def output(self):
        return [luigi.LocalTarget(os.path.join(os.getenv('TEMP_DIR'), file)) for file in Utils.params(self.file).files]

    def run(self):
        ext = os.path.splitext(self.input().path)[-1]
        if ext == '.rar':
            arch = RarFile(self.input().path)
        elif ext == '.zip':
            arch = zipfile.ZipFile(self.input().path)

        for f in arch.namelist():
            if os.path.basename(f) in [os.path.basename(out.path) for out in self.output()]:
                target = os.path.join(os.getenv('TEMP_DIR'), f)
                arch.extract(f, os.path.join(os.getenv('TEMP_DIR')))
                copyfile(target, os.path.join(os.getenv('TEMP_DIR'), os.path.basename(f)))


class ExtractArchives(luigi.Task):

    file = luigi.Parameter()

    def requires(self):
        return hdb.DownloadFiles(file=self.file)

    def output(self):
        return [luigi.LocalTarget(os.path.join(os.path.dirname(f.path), '{}.{}'.format(os.path.splitext(os.path.basename(f.path))[0], Utils.params(self.file).ext[1]))) for f in self.input()]

    def run(self):
        for idx, file in enumerate(self.input()):
            ext = os.path.splitext(file.path)[-1]
            if ext == '.rar':
                arch = RarFile(file.path)
            elif ext == '.zip':
                arch = zipfile.ZipFile(file.path)

            f = arch.namelist()[0]
            target = os.path.join(os.getenv('TEMP_DIR'), f)
            arch.extract(f, os.path.join(os.getenv('TEMP_DIR')))
            copyfile(target, os.path.join(os.getenv('TEMP_DIR'), os.path.basename(self.output()[idx].path)))
