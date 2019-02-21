import luigi
import asr_source_tasks as srcs


class Runner(luigi.WrapperTask):
    def requires(self):
        yield srcs.AsrUra()
        yield srcs.AsrKar()
        yield srcs.AsrAla()


if __name__ == '__main__':
    # start_time = time.time()
    luigi.run()
    # print(time.monotonic() - start_time)