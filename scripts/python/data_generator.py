from generator_factory import ConcreteGeneratorFactory, generate_and_export_all_events
import pathlib


if __name__ == "__main__":
  
    output_path = pathlib.Path("../../e-commerce-user-events")
    factory = ConcreteGeneratorFactory()
    generate_and_export_all_events(factory, output_path)
    
