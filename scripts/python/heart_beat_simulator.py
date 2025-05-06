from models import Simulator




if __name__ == "__main__":
    #Simulater Heart Beat of 4 customers
    simulator = Simulator(4)
    simulator.simulate()