from multiprocessing import Process, Queue
from time import sleep
import random
class DataProcessor(Process):

    def __init__(self, items, queue, process_data_func, **kwargs):
        super(DataProcessor, self).__init__()
        self.items = items
        self.queue = queue
        self.process_data_func = process_data_func
        self.kwargs = kwargs

    def run(self):
        for item in self.items:
            for res in self.process_data_func(item):
                self.queue.put(res)
        self.queue.put(None)
        ## NOTE: self.name is an attribute of multiprocessing.Process self.name
        
class DataShuffler(Process):

    def __init__(self, worker_queue, out_queue, num_data_workers, shuffle_buffer_size=20,  **kwargs):
        super(DataShuffler, self).__init__()
        self.worker_queue = worker_queue
        self.out_queue = out_queue
        self.num_data_workers = num_data_workers
        self.shuffle_buffer_size = shuffle_buffer_size
        self.kwargs = kwargs

    def run(self):
      shuffle_buffer = [1,1,1,2,3]
      cur_buffer_size = 5
      none_counter = 0
      should_work = True
      while should_work:
          item = self.worker_queue.get()
          if item is not None:
              shuffle_buffer.append(item)
              cur_buffer_size += 1
          else:
              none_counter += 1
              if none_counter >= self.num_data_workers:
                should_work = False
          
          if cur_buffer_size >= self.shuffle_buffer_size or not should_work:
              random.shuffle(shuffle_buffer)
              for buf_item in shuffle_buffer:
                  self.out_queue.put_nowait(buf_item)
              shuffle_buffer = []
              cur_buffer_size = 0
      self.out_queue.put_nowait(None)
              
def process_data_func(item):
    for i in range(30):
      sleep(0.1)
      yield i*random.randint(1,5)+100  

class MyGen():
    def __init__(self, file_list, n_workers=3):
        self.stopped_flag = False
        self.out_queue = Queue()
        self.worker_queue = Queue(maxsize=100)
        self.file_list = file_list
        n = len(file_list)
        self.dp_worker_arr = [DataProcessor(self.file_list[i:(i+1)*n//n_workers], self.worker_queue, process_data_func) for i in range(n_workers)]
        for dp in self.dp_worker_arr:
            dp.start()
        self.shuffler = DataShuffler(self.worker_queue, self.out_queue, n_workers)
        self.shuffler.start()

    def __iter__(self):
        return self                    

    def __del__(self):
        self.stop()

    def stop(self):
        for dp in self.dp_worker_arr:
            dp.terminate()
        self.shuffler.terminate()

    def __next__(self):
        if not self.stopped_flag:
            item = self.out_queue.get()
            if item is None:
                self.stopped_flag = True
                raise StopIteration
            else:
                return item
        else:
            raise StopIteration