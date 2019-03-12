import luigi
import http_govkz


class HttpRunner(luigi.WrapperTask):
    def requires(self):
        # yield http_govkz.HttpStatGovOKED(file="http_statgov_oked.json")
        # yield http_govkz.HttpStatGovKPVED(file="http_statgov_kpved.json")
        # yield http_govkz.HttpStatGovNVED(file="http_statgov_nved.json")
        # yield http_govkz.HttpStatGovKURK(file="http_statgov_kurk.json")
        # yield http_govkz.HttpStatGovMKEIS(file="http_statgov_mkeis.json")
        # yield http_govkz.ExtractArchive(file="http_statgov_kato.json")
        # #
        # yield http_govkz.HttpStatGovKATO(file="http_statgov_kato.json")
        # yield http_govkz.HttpKgdGovPseudoCompany(file="http_kgdgov_pseudo_company.json")
        # yield http_govkz.HttpKgdGovWrongAddress(file="http_kgdgov_wrong_address.json")
        # yield http_govkz.HttpKgdGovBankrupt(file="http_kgdgov_bankrupt.json")
        # yield http_govkz.HttpKgdGovInactive(file="http_kgdgov_inactive.json")
        # yield http_govkz.HttpKgdGovInvalidRegistration(file="http_kgdgov_invalid_registration.json")
        # yield http_govkz.HttpKgdGovViolationTaxCode(file="http_kgdgov_violation_tax_code.json")
        # yield http_govkz.HttpKgdGovViolationTaxCode(file="http_kgdgov_violation_tax_code.json")
        # yield http_govkz.HttpKgdGovTaxArrearsULOver150(file="http_kgdgov_tax_arrears_150.json")
        yield http_govkz.DownloadFiles(file="http_statgov_companies.json")
        # yield http_govkz.CollectExcelFileToCsv(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_oked.json")
        # yield http_govkz.DownloadFile(file="http_statgov_kpved.json")


if __name__ == '__main__':
    luigi.run()
