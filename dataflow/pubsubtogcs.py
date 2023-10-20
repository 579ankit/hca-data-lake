import argparse
from datetime import datetime
import logging
import random

from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
    Map   
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import fileio


class GroupMessagesByFixedWindows(PTransform):
    # A composite transform that groups Pub/Sub messages based on publish time
        
    def __init__(self, window_size, num_shards=2):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class ExtractMsgs(DoFn):
    #To Extract messages from key-value pairs
    def process(self,key_value):
        (key,value)=key_value
        yield from value
        
                
def hive_filenaming(*args):
  #hive file partitioning 
  file_name = fileio.destination_prefix_naming()(*args)
  destination = file_name.split('----')[0]  
  utc_now=datetime.utcnow()
  date_str=utc_now.strftime("%Y-%m-%d")
  time_str=utc_now.strftime("%H:%M")
  datekey="date="+date_str
  timekey="time="+time_str
  filedir="/".join([datekey,timekey]) + "/"
  filename = filedir+ destination + ".jsonl"
  return  filename              
            
def run(input_subscription, output_path,window_size,num_shards, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )
    
    destination_filename=input_subscription.split('/')[-1].split("-")[0]
    
    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription).with_output_types(bytes)
            | "UTF-8 bytes to string" >> Map(lambda message: message.decode("utf-8"))
            | "Window into" >> GroupMessagesByFixedWindows(window_size=window_size, num_shards=num_shards)
            | "Get messages">>ParDo(ExtractMsgs())
            | "Write to GCS">>fileio.WriteToFiles(path=output_path,destination=destination_filename,file_naming=hive_filenaming,shards=num_shards)
            
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub subscription to read from."
        '"projects/<PROJECT_ID>/subscriptions/<subscription_ID>".',
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file directory including the prefix.",
    )
    parser.add_argument(
        "--window_size",
        type=int,
        default=10,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=1,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.output_path,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )