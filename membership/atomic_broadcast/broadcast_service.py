import argparse


def main():
    args = build_args()

    for c in args:
        print(repr(c))
    pass


def build_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--port', type=int,
                        help='Port to start channel range on. Range will'
                             ' consist of (start port) -> (start port + f).')

    parser.add_argument('-f', '--faults', type=int,
                        help='Number of desired faults to tolerate.'
                             'This means that there will be (f+1) channels.')

    return parser.parse_args()


if __name__ == '__main__':
    main()
