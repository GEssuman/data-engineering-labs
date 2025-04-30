from generator_factory import ConcreteGeneratorFactory, generate_and_export_all_events
import pathlib
import time
import random

if __name__ == "__main__":

    while True:
  
        output_path = pathlib.Path("../../e-commerce-user-events")
        factory = ConcreteGeneratorFactory()
        generate_and_export_all_events(factory, output_path)

        time.sleep(float(random.uniform(0, 4)))
    
