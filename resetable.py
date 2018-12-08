import threading
import time
import random

class ResetableTimer(threading.Thread):
	"""docstring for ResetableTimer"""
	def __init__(self, timeout, alert):
		super(ResetableTimer, self).__init__()
		self.timeout = timeout
		self.alert = alert
		self.reset = threading.Event()
		self.kill = False
	
	def run(self):
		while True:
			if self.kill: break
			while not self.reset.wait(self.timeout):
				self.alert.set()
			else:
				self.reset.clear()

	def resetTimer(self):
		self.reset.set()

	def stopTimer(self):
		self.reset.set()
		self.kill = True

class RepeatedTask(threading.Thread):
	"""docstring for RepeatedTask"""
	def __init__(self, timeout, target):
		super(RepeatedTask, self).__init__()
		self.timeout = timeout
		self.__name__ = target.__name__
		self.__f = target
		self.alert = threading.Event()
		self.rtimer = ResetableTimer(self.timeout, self.alert)
		self.rtimer.start()
		self.kill = False

	def run(self):
		while not self.kill:
			if self.alert.is_set():
				self.alert.clear()
				self.__f()

	def stop(self):
		self.rtimer.stopTimer()
		self.kill = True

def f():
	print("inside f")

def main():
	task = RepeatedTask(3,f)
	task.start()

	try:
		task.join()
	except KeyboardInterrupt:
		task.stop()

	# time.sleep(10)
	# task.stop()

if __name__ == '__main__':
	main()