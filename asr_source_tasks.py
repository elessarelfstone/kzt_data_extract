import luigi
import asr_table_tasks as att


class AsrUra(luigi.WrapperTask):
    """
    Задание на выгрузку таблиц из АСР Уральска
    """
    def requires(self):
        file = "asr_ura.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrKar(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Караганда
    """
    def requires(self):
        file = "asr_kar.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrAla(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Алматы
    """
    def requires(self):
        file = "asr_ala.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrAkt(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Актау
    """
    def requires(self):
        file = "asr_akt.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrAktb(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Актобе
    """
    def requires(self):
        file = "asr_aktb.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrAst(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Астана
    """
    def requires(self):
        file = "asr_ast.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")


class AsrAtr(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Атырау
    """
    def requires(self):
        file = "asr_atr.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrKok(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Кокшетау
    """
    def requires(self):
        file = "asr_kok.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrKos(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Костанай
    """
    def requires(self):
        file = "asr_kos.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrKzl(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Кызылорда
    """
    def requires(self):
        file = "asr_kzl.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrOsk(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Усть-каменогорск
    """
    def requires(self):
        file = "asr_osk.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrPav(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Павлодар
    """
    def requires(self):
        file = "asr_pav.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrPet(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Петроваловск
    """
    def requires(self):
        file = "asr_pet.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrSem(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Семей
    """
    def requires(self):
        file = "asr_sem.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrShm(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Шимкент
    """
    def requires(self):
        file = "asr_shm.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrTal(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Талдыкорган
    """
    def requires(self):
        file = "asr_tal.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")


class AsrTar(luigi.WrapperTask):
    """
    # Задание на выгрузку таблиц из АСР Тараз
    """
    def requires(self):
        file = "asr_tar.json"
        yield att.AsrTdrTable(file=file, table="db.abonent")
        yield att.AsrAbonentTable(file=file, table="db.tdr")
        yield att.AsrDeviceTable(file=file, table="db.device")
        yield att.AsrAddressTable(file=file, table="db.address")
        yield att.AsrBillTable(file=file, table="db.bill")
        yield att.AsrPaymentTable(file=file, table="db.payment")
        yield att.AsrSaldoTable(file=file, table="db.saldo")