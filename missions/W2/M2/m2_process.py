from multiprocessing import Process

class Continent :
    def __init__(self, name = "Asia"):
        self.name = name

def print_continent(continent : Continent):
    print("The name of continent is : {name}".format(name = continent.name))

if __name__ == "__main__":
    continents = [
        Continent(), Continent("America"),
        Continent("Europe"), Continent("Africa")
    ]
    processList = []
    for continent in continents:
        processList.append(Process(target=print_continent, args = (continent,)))
   
    for process in processList:
        process.start()

    for process in processList:
        process.join()