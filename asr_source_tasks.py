import luigi
import asr_table_tasks as att


class AsrUra(luigi.WrapperTask):
    def requires(self):
        file = "asr_ura.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")


class AsrKar(luigi.WrapperTask):
    def requires(self):
        file = "asr_kar.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")


class AsrAla(luigi.WrapperTask):
    def requires(self):
        file = "asr_ala.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")