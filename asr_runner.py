import luigi
import time
from datetime import timedelta
import asr_source_tasks as srcs


class AsrRunner(luigi.WrapperTask):
    def requires(self):
        yield srcs.AsrUra()
        # yield srcs.AsrKar()
        # yield srcs.AsrAla()
        # yield srcs.AsrAktb()
        # yield srcs.AsrAkt()
        # yield srcs.AsrAst()
        # yield srcs.AsrKok()
        # yield srcs.AsrKos()
        # yield srcs.AsrKzl()
        # yield srcs.AsrOsk()
        # yield srcs.AsrPav()
        # yield srcs.AsrPet()
        # yield srcs.AsrSem()
        # yield srcs.AsrShm()
        # yield srcs.AsrTal()
        # yield srcs.AsrTar()


if __name__ == '__main__':
    start_time = time.monotonic()
    luigi.run()
    end_time = time.monotonic()
    print(timedelta(seconds=end_time - start_time))