import luigi
import time
import os
import logging 
import argparse

def get_args():
    description = """luigi demo"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--id', dest="id", type=int)
    args, unknownargs = parser.parse_known_args()

    return args
class GlobalParams(luigi.Config):
    id = luigi.IntParameter(default = 5)

class int_range(luigi.Task):
    cur_time = 20211206
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget(f'int_range_{self.cur_time}.txt')
    def run(self):
        with self.output().open('w') as outfile:
            for i in range(5):
                time.sleep(1)
                print(i)
                outfile.write(f'{i}\n')
        time.sleep(2)

class square_int_range(luigi.Task):
    id = GlobalParams().id
    cur_time = 20211206
    def requires(self):
        return int_range()
    def output(self):
        return luigi.LocalTarget(os.path.dirname(self.input().path) + f'square_int_range_{self.cur_time}.txt')
    def run(self):
        print("ID",id)
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile:
                n = int(line.strip())
                outfile.write(f"{n*n}\n")
        time.sleep(5)

if __name__ == '__main__':
    # args = get_args()
    # luigi.run(['square_int_range', '--workers', '1', '--scheduler-host','localhost'
            #    '--GlobalParams-id', f"{args.id}"])
    luigi.run()