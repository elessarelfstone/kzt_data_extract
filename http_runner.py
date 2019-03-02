import luigi
import http_govkz


class HttpRunner(luigi.WrapperTask):
    def requires(self):
        yield http_govkz.CollectToCsv(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_kpved.json")


if __name__ == '__main__':
    luigi.run()
