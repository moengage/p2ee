import time
import uuid

from p2ee.orm.models.base.docs.simple import SimpleDocument
from p2ee.orm.models.base.fields import ObjectField, ListField, DictField


class MultiThreadPoolExecutor(object):
    def __init__(self, max_threads, thread_task_list, iteration_sleep=0.1):
        """

        :param max_threads: Maximum number of execution threads required
        :param thread_task_list: List of ThreadTask objects
        :param iteration_sleep: Sleep time for threads in wait
        """
        self.max_threads = max_threads
        self.thread_task_list = thread_task_list
        self.finished_threads = 0
        self.started_threads = 0
        self.running_threads = {}
        self.is_running = True
        self.params_count = 0
        self.iteration_sleep = iteration_sleep

    def get_threads_to_run(self):
        for thread_task in self.thread_task_list:
            self.params_count += 1
            yield thread_task.thread_class(*thread_task.args, **thread_task.kwargs)

    def can_execute_thread(self):
        return len(self.running_threads) < self.max_threads

    def execute_thread(self, thread):
        if not self.is_running:
            raise Exception('ThreadPool is stopped')
        thread.start()
        self.started_threads += 1
        self.running_threads[uuid.uuid4().hex[:8]] = thread

    def get_completed_thread(self):
        for thread_id, thread in self.running_threads.items():
            if not thread.isAlive():
                self.running_threads.pop(thread_id)
                self.finished_threads += 1
                yield thread
            else:
                time.sleep(self.iteration_sleep)

    def start(self):
        for thread in self.get_threads_to_run():
            while not self.can_execute_thread():
                for completed_thread in self.get_completed_thread():
                    yield completed_thread
            self.execute_thread(thread)
        while len(self.running_threads) > 0:
            for completed_thread in self.get_completed_thread():
                yield completed_thread

        if self.finished_threads != self.params_count:
            raise Exception('Finished threads and params count do not match')


class ThreadPoolExecutor(MultiThreadPoolExecutor):
    def __init__(self, max_threads, thread_class, params_list, iteration_sleep=0.1):
        thread_task_list = self.__get_thread_task_list(thread_class, params_list)
        super(ThreadPoolExecutor, self).__init__(max_threads, thread_task_list, iteration_sleep)

    @classmethod
    def __get_thread_task_list(cls, thread_class, params_list):
        for args in params_list:
            yield ThreadTask(thread_class=thread_class, args=args)


class ThreadTask(SimpleDocument):
    thread_class = ObjectField()
    args = ListField(default=list)
    kwargs = DictField(default=dict)
