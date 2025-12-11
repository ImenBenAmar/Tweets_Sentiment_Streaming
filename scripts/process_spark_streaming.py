import os
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf, lit, when,
    regexp_replace, to_timestamp, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from textblob import TextBlob
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BlueskySentimentSparkPipeline:
    """Spark Structured Streaming pipeline for Bluesky sentiment analysis."""
    
    def __init__(self):
        """Initialize the pipeline with configuration."""
        load_dotenv()
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.input_topic = os.getenv('KAFKA_TOPIC', 'twitterdata') 
        self.output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'twitterdata-clean')
        
        # Spark configuration
        self.checkpoint_location = os.getenv(
            'SPARK_CHECKPOINT_DIR', 
            '/tmp/spark-checkpoint'
        )
        self.app_name = os.getenv('SPARK_APP_NAME', 'BlueskySentimentAnalysis')
        
        # Initialize Spark session
        self.spark: Optional[SparkSession] = None
        
        logger.info("Pipeline configuration loaded")
    
    def _get_bluesky_schema(self) -> StructType:
        """
        Define the schema for incoming Bluesky data.
        """
        return StructType([
            StructField("text", StringType(), True),
            StructField("created_at", StringType(), True)
        ])
    
    def _initialize_spark_session(self) -> None:
        """Initialize Spark session."""
        try:
            # UPDATED VERSION HERE: Changed 3.2.1 to 3.5.0 to match your Spark 3.5.6
            self.spark = (SparkSession.builder
                         .appName(self.app_name)
                         .config("spark.jars.packages", 
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                         .config("spark.sql.streaming.checkpointLocation", 
                                self.checkpoint_location)
                         .config("spark.streaming.stopGracefullyOnShutdown", "true")
                         .config("spark.sql.shuffle.partitions", "4")
                         .getOrCreate())
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Spark session initialized: {self.app_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def _preprocess_text(self, df: DataFrame) -> DataFrame:
        """
        Preprocess text by removing URLs, special characters, etc.
        """
        try:
            # Filter out null text
            df = df.filter(col('text').isNotNull())
            
            # Remove URLs
            df = df.withColumn('text', regexp_replace('text', r'http\S+', ''))
            
            # Remove newlines
            df = df.withColumn('text', regexp_replace('text', r'[\n\r]', ' '))
            
            # Remove extra spaces
            df = df.withColumn('text', regexp_replace('text', r'\s+', ' '))
            
            # Filter out empty text after preprocessing
            df = df.filter(col('text') != '')
            
            return df
            
        except Exception as e:
            logger.error(f"Error in text preprocessing: {e}")
            raise
    
    @staticmethod
    def _calculate_polarity(text: str) -> float:
        try:
            return float(TextBlob(text).sentiment.polarity)
        except Exception:
            return 0.0
    
    @staticmethod
    def _calculate_subjectivity(text: str) -> float:
        try:
            return float(TextBlob(text).sentiment.subjectivity)
        except Exception:
            return 0.0
    
    def _perform_sentiment_analysis(self, df: DataFrame) -> DataFrame:
        """Perform sentiment analysis on text."""
        try:
            # Register UDFs
            polarity_udf = udf(self._calculate_polarity, FloatType())
            subjectivity_udf = udf(self._calculate_subjectivity, FloatType())
            
            # Calculate polarity
            df = df.withColumn("polarity_v", polarity_udf(col("text")))
            
            # Classify sentiment
            df = df.withColumn(
                'polarity',
                when(col('polarity_v') > 0, lit('Positive'))
                .when(col('polarity_v') == 0, lit('Neutral'))
                .otherwise(lit('Negative'))
            )
            
            # Calculate subjectivity
            df = df.withColumn("subjectivity_v", subjectivity_udf(col("text")))
            
            df = df.withColumn(
                'subjectivity',
                when(col('subjectivity_v') >= 0.5, lit('Subjective'))
                .otherwise(lit('Objective'))
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            raise
    
    def _read_from_kafka(self) -> DataFrame:
        """Read streaming data from Kafka."""
        try:
            df = (self.spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", self.input_topic)
                  .option("startingOffsets", "latest")
                  .load())
            
            logger.info(f"Reading from Kafka topic: {self.input_topic}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
    
    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """Parse Kafka messages and extract Bluesky data."""
        try:
            # Cast key and value to string
            df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            
            # Parse JSON value using the SIMPLE schema
            schema = self._get_bluesky_schema()
            df = df.withColumn("data", from_json(col("value"), schema))
            df = df.select("data.*")
            
            # Bluesky sends ISO timestamps (e.g. 2024-12-01T10:00:00Z)
            # Spark handles this automatically with to_timestamp
            df = df.withColumn("ts", to_timestamp(col("created_at")))
            
            # If timestamp is null, use current time
            df = df.withColumn(
                "ts",
                when(col("ts").isNull(), current_timestamp())
                .otherwise(col("ts"))
            )
            
            # Watermark required for aggregation (if added later)
            df = df.withWatermark("ts", "10 seconds")
            
            return df
            
        except Exception as e:
            logger.error(f"Error parsing Kafka messages: {e}")
            raise
    
    def _prepare_output_data(self, df: DataFrame) -> DataFrame:
        """Prepare data for output to Kafka."""
        try:
            # We removed fields that don't exist anymore (user_name, retweet_count, etc.)
            # We keep only what we have.
            output_df = df.select(
                to_json(struct(
                    'text', 
                    'ts', 
                    'polarity_v', 
                    'polarity', 
                    'subjectivity_v', 
                    'subjectivity'
                )).alias("value")
            )
            
            return output_df
            
        except Exception as e:
            logger.error(f"Error preparing output data: {e}")
            raise
    
    def _write_to_kafka(self, df: DataFrame) -> None:
        """Write processed data to Kafka output topic."""
        try:
            query = (df
                    .writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                    .option("topic", self.output_topic)
                    .option("checkpointLocation", f"{self.checkpoint_location}/kafka_output")
                    .outputMode("append")
                    .start())
            
            logger.info(f"Writing to Kafka topic: {self.output_topic}")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error writing to Kafka: {e}")
            raise
    
    def run(self, output_mode: str = "kafka") -> None:
        """Run the pipeline."""
        try:
            self._initialize_spark_session()
            logger.info("Starting Bluesky Spark Pipeline...")
            
            raw_df = self._read_from_kafka()
            parsed_df = self._parse_kafka_messages(raw_df)
            preprocessed_df = self._preprocess_text(parsed_df)
            analyzed_df = self._perform_sentiment_analysis(preprocessed_df)
            output_df = self._prepare_output_data(analyzed_df)
            
            if output_mode == "console":
                query = output_df.writeStream.outputMode("append").format("console").start()
                query.awaitTermination()
            else:
                self._write_to_kafka(output_df)
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        if self.spark:
            self.spark.stop()

def main():
    pipeline = BlueskySentimentSparkPipeline()
    # Tu peux changer "kafka" en "console" pour tester l'affichage dans le terminal
    pipeline.run(output_mode="kafka")

if __name__ == "__main__":
    main()