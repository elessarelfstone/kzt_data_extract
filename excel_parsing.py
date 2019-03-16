import os
import pandas as pd
from utils import Utils


class ExcelParsing():

    @staticmethod
    def parse_file(params_file, xl_file_path):
        """

        :param params_file: file with parameters
        :param xl_file_path:
        :return:
        """
        data = pd.DataFrame()
        xls = pd.ExcelFile(xl_file_path)
        xls_sheets = xls.sheet_names
        for sh in Utils.params(params_file).sheets:
            df = pd.read_excel(xl_file_path,
                               sheet_name=xls_sheets[sh],
                               skiprows=Utils.params(params_file).skiprows,
                               index_col=None,
                               dtype=str,
                               header=None)

            data = data.append(df, ignore_index=True)
        data = data.replace(['nan', 'None'], '', regex=True)

        return data

    @staticmethod
    def parse_files(json_file, xl_files):
        all_data = pd.DataFrame()
        for f_path in xl_files:
            data = ExcelParsing.parse_file(json_file, f_path)
            all_data = all_data.append(data, ignore_index=True)

        return all_data





