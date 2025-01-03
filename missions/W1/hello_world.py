import functools
import time


def recusrive(N):
    if N == 0:
        return 0
    else:
        return recusrive(N - 1) + N


def main():
    recusrive(3)


if __name__ == "__main__":
    main()
