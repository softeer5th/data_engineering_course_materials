from multiprocessing import Process


def print_continent(continent=None):
    if not continent:
        continent = "Asia"
    print(continent)


def main():
    p1 = Process(target=print_continent)
    p2 = Process(target=print_continent, args=("America",))
    p3 = Process(target=print_continent, args=("Europe",))
    p4 = Process(target=print_continent, args=("Africa",))

    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()


if __name__ == "__main__":
    main()
