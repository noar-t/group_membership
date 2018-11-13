import socket

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 51519))
    s.sendall('abc'.encode('UTF-8'))
    data = s.recv(1024)
    s.close()
    print('Received' + repr(data))

if __name__ == '__main__':
    main()
