import luigi
import http_statgovkz
import http_kgdgovkz


class HttpRunner(luigi.WrapperTask):
    def requires(self):

        # -----statgov--------
        yield http_statgovkz.HttpStatGovOKED(file="http_statgov_oked.json")
        # yield http_statgovkz.HttpStatGovKPVED(file="http_statgov_kpved.json")
        # yield http_statgovkz.HttpStatGovNVED(file="http_statgov_nved.json")
        # yield http_statgovkz.HttpStatGovKURK(file="http_statgov_kurk.json")
        # yield http_statgovkz.HttpStatGovMKEIS(file="http_statgov_mkeis.json")
        # yield http_statgovkz.HttpStatGovKATO(file="http_statgov_kato.json")
        # yield http_statgovkz.HttpStatGovCompanies(file="http_statgov_companies.json")
        #
        # # -----kgdbgov--------
        # yield http_kgdgovkz.HttpKgdGovPseudoCompany(file="http_kgdgov_pseudo_company.json")
        # yield http_kgdgovkz.HttpKgdGovWrongAddress(file="http_kgdgov_wrong_address.json")
        # yield http_kgdgovkz.HttpKgdGovBankrupt(file="http_kgdgov_bankrupt.json")
        # yield http_kgdgovkz.HttpKgdGovInactive(file="http_kgdgov_inactive.json")
        # yield http_kgdgovkz.HttpKgdGovInvalidRegistration(file="http_kgdgov_invalid_registration.json")
        # yield http_kgdgovkz.HttpKgdGovViolationTaxCode(file="http_kgdgov_violation_tax_code.json")
        # yield http_kgdgovkz.HttpKgdGovTaxArrearsULOver150(file="http_kgdgov_tax_arrears_150.json")



if __name__ == '__main__':
    luigi.run()