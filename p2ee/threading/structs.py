import threading


class Lockable(object):
    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_lock()

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()


class LockingDict(Lockable):
    def __init__(self):
        super(LockingDict, self).__init__()
        self.dict = {}

    def get_dict(self):
        return self.dict

    def set_dict(self, dictionary):
        self.dict = dictionary


class LockingSet(Lockable):
    def __init__(self):
        super(LockingSet, self).__init__()
        self.set = set()

    def add(self, item):
        self.set.add(item)

    def add_all(self, items):
        self.set.update(items)

    def remove(self, item):
        self.set.remove(item)

    def remove_all(self, items):
        self.set = self.set - set(items)

    def get_set(self):
        return self.set


class Barrier(object):
    def __init__(self, n):
        self.n = n
        self.count = 0
        self.mutex = threading.Semaphore(1)
        self.barrier = threading.Semaphore(0)

    def wait(self):
        self.mutex.acquire()
        self.count += 1
        self.mutex.release()
        if self.count == self.n:
            self.barrier.release()
        self.barrier.acquire()
        self.barrier.release()