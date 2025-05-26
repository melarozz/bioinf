from clearml import PipelineDecorator

# Define a pipeline component using the decorator
@PipelineDecorator.component()
def hello_world_component():
    print("Hello world")

# Define the pipeline structure
@PipelineDecorator.pipeline(
    name='hello_world_pipeline',
    project='Hello World Pipelines',
    version='0.1'
)
def pipeline_definition():
    # Connect the components in execution order
    hello_world_component()

if __name__ == '__main__':
    # Run the pipeline locally (without queuing)
    PipelineDecorator.run_locally()
    
    # Execute the pipeline
    pipeline_definition()
