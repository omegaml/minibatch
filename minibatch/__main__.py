import logging
from time import sleep

from minibatch import connectdb
from minibatch.models import ThreadAlive, Monitor

alive = ThreadAlive()
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%H:%M:%S', )


def monitor():
    monitor = Monitor()
    while alive:
        print("*** Participants ***")
        for participant in monitor.participants():
            print(repr(participant))
        sleep(5)


def main():
    print('minibatch!')
    connectdb()
    monitor()


if __name__ == '__main__':
    main()
