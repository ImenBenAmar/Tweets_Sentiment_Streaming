import os
import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToInfluxDBPipeline:
    """Pipeline to consume Bluesky data from Kafka and write to InfluxDB."""
    
    def __init__(self):
        """Initialize the pipeline with configuration from environment variables."""
        load_dotenv()
        
        # Kafka configuration
        # Must match the 'output_topic' from your process.py
        self.topic_name = os.getenv('KAFKA_OUTPUT_TOPIC', 'twitterdata-clean')
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'bluesky-influx-group')
        
        # InfluxDB configuration
        self.influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        self.influx_token = os.getenv('influxdb_token')
        self.influx_org = os.getenv('influxdb_org')
        self.influx_bucket = os.getenv('influxdb_bucket')
        
        # Validate configuration
        self._validate_config()
        
        # Initialize clients (to be set in run method)
        self.consumer: Optional[KafkaConsumer] = None
        self.influx_client: Optional[InfluxDBClient] = None
        self.write_api: Optional[WriteApi] = None
        
        # Statistics
        self.messages_processed = 0
        self.messages_failed = 0
    
    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_vars = {
            'influxdb_token': self.influx_token,
            'influxdb_org': self.influx_org,
            'influxdb_bucket': self.influx_bucket
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def _initialize_kafka_consumer(self) -> None:
        """Initialize Kafka consumer with proper configuration."""
        try:
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest', # Start from beginning if new group
                enable_auto_commit=True,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"Kafka consumer initialized for topic: {self.topic_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def _initialize_influxdb_client(self) -> None:
        """Initialize InfluxDB client and write API."""
        try:
            self.influx_client = InfluxDBClient(
                url=self.influx_url,
                token=self.influx_token,
                org=self.influx_org
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            logger.info("InfluxDB client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB client: {e}")
            raise
    
    def _create_data_point(self, message_value: Dict[str, Any]) -> Point:
        """
        Create an InfluxDB Point from message data.
        Matches the output structure from process.py
        """
        # Ensure we don't have None values for fields that require strings/floats
        polarity_val = float(message_value.get('polarity_v', 0.0))
        subjectivity_val = float(message_value.get('subjectivity_v', 0.0))
        
        # InfluxDB Measurement Name
        point = Point("bluesky_post")
        
        # TAGS: Strings used for filtering/grouping in Grafana (indexed)
        point.tag("polarity_cat", message_value.get('polarity', 'Neutral'))
        point.tag("subjectivity_cat", message_value.get('subjectivity', 'Objective'))
        
        # FIELDS: Values used for calculations/graphs (not indexed)
        point.field("polarity_val", polarity_val)
        point.field("subjectivity_val", subjectivity_val)
        point.field("text", str(message_value.get('text', '')))
        
        # TIME: Use the timestamp from Bluesky/Spark
        # Spark outputs ISO string, InfluxDB Client handles this automatically
        if message_value.get('ts'):
            point.time(message_value.get('ts'))
            
        return point
    
    def _process_message(self, message) -> bool:
        """Process a single Kafka message and write to InfluxDB."""
        try:
            message_value = message.value
            
            # Create and write data point
            data_point = self._create_data_point(message_value)
            self.write_api.write(bucket=self.influx_bucket, record=data_point)
            
            self.messages_processed += 1
            
            # Log every 10 messages so you see it's working in terminal
            if self.messages_processed % 10 == 0:
                logger.info(f"Processed {self.messages_processed} messages. Last text: {message_value.get('text', '')[:30]}...")
            
            return True
            
        except KeyError as e:
            logger.error(f"Missing required field in message: {e}")
            self.messages_failed += 1
            return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.messages_failed += 1
            return False
    
    def run(self) -> None:
        """Run the pipeline."""
        try:
            self._initialize_kafka_consumer()
            self._initialize_influxdb_client()
            
            logger.info("Starting Bluesky -> InfluxDB Consumer...")
            
            for message in self.consumer:
                self._process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.error(f"Unexpected error in pipeline: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        logger.info(
            f"Pipeline statistics - "
            f"Processed: {self.messages_processed}, "
            f"Failed: {self.messages_failed}"
        )
        
        if self.consumer:
            self.consumer.close()
        
        if self.write_api:
            self.write_api.close()
        
        if self.influx_client:
            self.influx_client.close()

def main():
    try:
        pipeline = KafkaToInfluxDBPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}")
        exit(1)

if __name__ == "__main__":
    main()