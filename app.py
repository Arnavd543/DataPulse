#!/usr/bin/env python3
import os

import aws_cdk as cdk

from datapulse.datapulse_stack import DataPulseStack


app = cdk.App()
DataPulseStack(app, "DataPulseStack")

app.synth()
