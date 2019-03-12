import os
import pandas as pd
from utils import Utils


class ExcelParsing():

    @staticmethod
    def parse_file(json_file, xl_file):
        data = pd.DataFrame()
        xls = pd.ExcelFile(xl_file)
        xls_sheets = xls.sheet_names
        for sh in Utils.params(json_file).sheets:
            df = pd.read_excel(xl_file,
                               sheet_name=xls_sheets[sh],
                               skiprows=Utils.params(json_file).skiprows,
                               index_col=None,
                               dtype=str,
                               header=None)

            data = data.append(df, ignore_index=True)
        data = data.replace(['nan', 'None'], '', regex=True)

        return data





