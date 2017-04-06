import os
import time
from multiprocessing import Pool

import DBaws


def long_time_task(name, num_samples):
    print('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    DBaws.mongodb_read_throughput(num_samples)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))

if __name__=='__main__':
    print('Parent process %s.' % os.getpid())
    num_samples =3000
    num_processes =5
    start = time.time()
    p = Pool()
    for i in range(num_processes):
        p.apply_async(long_time_task, args=(i, num_samples))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    end = time.time()
    throughput = num_processes * num_samples / (end - start)
    print(throughput)