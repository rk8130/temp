from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import numpy as np


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))



# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-7prvp.centralindia.azure.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'Y4AUFZP4SRDY4Z5Y',
    'sasl.password': 'YgiNuaa+ObPVzi1ap2y2gx0ZGribRT8Miy0qzNMu8kRj5AXRYZoc8Lw+/0R7XOR0'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-lo3do.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('6NIZUTBC6SJCWNNB', 'LlZZDvA7BvgbBYGFoWbSpyQHlpXjHG2R89o81ahdZ4es6JooNPkjkMc7VALMY15I')
})

# Fetch the latest Avro schema for the value
subject_name = 'topic_0-value'   
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})



# Load the CSV data into a pandas DataFrame
df = pd.read_csv('delivery_trip_truck_data.csv')

# print(df.dtypes)

# GpsProvider                            object
# BookingID                              object
# Market/Regular                         object
# BookingID_Date                         object
# vehicle_no                             object
# Origin_Location                        object
# Destination_Location                   object
# Org_lat_lon                            object
# Des_lat_lon                            object
# Data_Ping_time                         object
# Planned_ETA                            object
# Current_Location                       object
# DestinationLocation                    object
# actual_eta                             object
# Curr_lat                              float64
# Curr_lon                              float64
# ontime                                 object
# delay                                  object
# OriginLocation_Code                    object
# DestinationLocation_Code               object
# trip_start_date                        object
# trip_end_date                          object
# TRANSPORTATION_DISTANCE_IN_KM         float64
# vehicleType                            object
# Minimum_kms_to_be_covered_in_a_day    float64
# Driver_Name                            object
# Driver_MobileNo                       float64
# customerID                             object
# customerNameCode                       object
# supplierID                             object
# supplierNameCode                       object
# Material Shipped                       object
# dtype: object


# handle missing values
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].fillna("NA")
    elif df[col].dtype in ['float64', 'int64']:
        df[col] = df[col].replace({np.nan: None})


df.rename(columns={'Market/Regular ': 'Market_Regular'}, inplace=True)


# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # print(value)
    # Produce to Kafka
    producer.produce(topic='topic_0', key=str(index), value=value, on_delivery=delivery_report)
    producer.flush()
    time.sleep(2)
    #break

print("All Data successfully published to Kafka")
