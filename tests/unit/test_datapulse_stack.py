import aws_cdk as core
import aws_cdk.assertions as assertions

from datapulse.datapulse_stack import DatapulseStack

# example tests. To run these tests, uncomment this file along with the example
# resource in datapulse/datapulse_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DatapulseStack(app, "datapulse")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
