import luigi
import http_govkz


class HttpRunner(luigi.WrapperTask):
    def requires(self):
        # yield http_govkz.HttpStatGovOKED(file="http_statgov_oked.json")
        # yield http_govkz.HttpStatGovKPVED(file="http_statgov_kpved.json")
        # yield http_govkz.HttpStatGovNVED(file="http_statgov_nved.json")
        # yield http_govkz.HttpStatGovKURK(file="http_statgov_kurk.json")
        # yield http_govkz.HttpStatGovMKEIS(file="http_statgov_mkeis.json")

        yield http_govkz.

        # yield http_govkz.CollectExcelFileToCsv(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_kpved.json")


if __name__ == '__main__':
    luigi.run()
