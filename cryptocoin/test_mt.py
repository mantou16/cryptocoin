import threading, time  
import Queue
import threading
'''
def doWaiting():  
    print 'start waiting1: ' + time.strftime('%H:%M:%S') + "\n"  
    time.sleep(3)  
    print 'stop waiting1: ' + time.strftime('%H:%M:%S') + "\n" 
def doWaiting1():  
    print 'start waiting2: ' + time.strftime('%H:%M:%S') + "\n"   
    time.sleep(8)  
    print 'stop waiting2: ', time.strftime('%H:%M:%S') + "\n"  
tsk = []    
thread1 = threading.Thread(target = doWaiting)  
thread1.start()  
tsk.append(thread1)
thread2 = threading.Thread(target = doWaiting1)  
thread2.start()  
tsk.append(thread2)
print 'start join: ' + time.strftime('%H:%M:%S') + "\n"   
for tt in tsk:
    tt.join(2)
print 'end join: ' + time.strftime('%H:%M:%S') + "\n"

def basic_worker(queue):
    while True:
        item = queue.get()
        # do_work(item)
        print(item)
        queue.task_done()
def basic():
    # http://docs.python.org/library/queue.html
    queue = Queue.Queue()
    for i in range(3):
         t = threading.Thread(target=basic_worker,args=(queue,))
         #t.daemon = True
         t.start()
    for item in range(4):
        queue.put(item)
    queue.join()       # block until all tasks are done
    print('got here')

basic()
'''
from threading import current_thread

message = Queue.Queue()


def producer(i):
    message.put(i)
    print 'put ' + str(i) + '\n'


def consumer():
	print current_thread().name
	while not message.empty():
	    msg = message.get()
	    print current_thread().name + ': ' + str(msg)
	    message.task_done()


threads = [threading.Thread(target=producer, args=(i,)) for i in range(12)]
for thread in threads:
	thread.start()
for thread in threads:
	thread.join()


threads = [threading.Thread(target=consumer, args=()) for i in range(10)]
for thread in threads:
	thread.setDaemon(True)
	thread.start()
for thread in threads:
	thread.join()
