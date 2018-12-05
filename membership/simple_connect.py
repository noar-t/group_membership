import socket
import pickle
import time
import group


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('velvetworm', 55555))
    #test_str = ''
    #for i in range(2000):
    #    test_str += str(i) + ', '
    #s.sendall(test_str.encode('UTF-8'))
    test_list = ['a', 'b', 'c']
    print(repr(pickle.dumps(test_list)))
    s.sendall(b'test')
#    print(s.recv(1024))
    s.sendall(pickle.dumps(['a', 'b', 'c']))
    s.close()


if __name__ == '__main__':
    main()
