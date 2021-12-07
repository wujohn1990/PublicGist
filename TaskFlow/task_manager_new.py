import luigi
import time
import os
import logging 


class int_range(luigi.Task):
    cur_time = 20211206
    # d= luigi.IntParameter()
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(f'int_range_{self.cur_time}.txt')
    def run(self):
        with self.output().open('w') as outfile:
            for i in range(5):
                time.sleep(2)
                print(i)
                outfile.write(f'{i}\n')
        time.sleep(2)
        with self.output().open('w') as outfile:
            for i in range(5):
                time.sleep(2)
                print(i)
                outfile.write(f'{i}\n')

class cube_int_range(luigi.Task):
    d = luigi.IntParameter()
    cur_time = 20211206
    def requires(self):
        return int_range()
    def output(self):
        return luigi.LocalTarget(os.path.dirname(self.input().path) + f'cube_int_range_{self.cur_time}.txt')
    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile:
                n = int(line.strip())
                outfile.write(f"{n*n*n}\n")
        time.sleep(5)

if __name__ == '__main__':
    luigi.run()