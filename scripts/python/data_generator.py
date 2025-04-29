from generator_factory import ConcreteGeneratorFactory, generate_and_export_all_events
import pathlib


if __name__ == "__main__":
  



    output_path = pathlib.Path("../../e-commerce-user-events")
    factory = ConcreteGeneratorFactory()
    generate_and_export_all_events(factory, output_path)
    
    
    
    
    # generator = ProductViewGenerator()
    # generator_2 = ProductPurchaseGenerator()

    # generator.prepare_event()
    # generator.do_export(folder_path)


    # generator_2.prepare_event()
    # generator_2.do_export(folder_path_2)