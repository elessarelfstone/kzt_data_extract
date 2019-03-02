import json
import os
from collections import namedtuple

class Utils():

    @staticmethod
    def load_params(file):
        """
        Load parameters for task from json file
        :param file:
        :return:
        """
        with open(file, "r") as f:
            raw_params = f.read()

        return raw_params

    @staticmethod
    def params_file_path(file):
        """
        Get path for params file
        :param file: json file with params
        :return: string
        """
        task_type = str(file).split("_")[0]
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'params', task_type, file)


    @staticmethod
    def params_named_tuple(file_path):
        """
        Get params as namedtuple
        :param file_path: path of params file
        :return: namedtuple object
        """
        p = json.loads(Utils.load_params(file_path))
        Params = namedtuple("Params", sorted(p))
        return Params(**p)

    @staticmethod
    def params(file):
        """
        Get params as namedtuple by just name of params file
        :param file:
        :return:
        """
        return Utils.params_named_tuple(Utils.params_file_path(file))

