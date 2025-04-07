from threading import Condition

class ReadWriteLock:
    def __init__(self):
        self.lock = Condition()
        self.readers = 0
        self.writer = False
        
    def acquire_read(self):
        with self.lock:
            while self.writer:
                self.lock.wait()
            self.readers += 1
            
    def release_read(self):
        with self.lock:
            self.readers -= 1
            if self.readers == 0:
                self.lock.notify_all()
                
    def acquire_write(self):
        with self.lock:
            while self.readers > 0 or self.writer:
                self.lock.wait()
            self.writer = True
    
    def release_write(self):
        with self.lock:
            self.writer = False
            self.lock.notify_all()