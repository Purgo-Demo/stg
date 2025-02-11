# Install the deep-translator library
%pip install deep-translator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from deep_translator import GoogleTranslator

# Define a UDF to translate text to English
def translate_to_english(text):
    if text is None or text.strip() == "":
        return text
    try:
        translated = GoogleTranslator(source='auto', target='en').translate(text)
        return translated
    except Exception as e:
        # Log the error message
        print(f"Translation error for text '{text}': {str(e)}")
        return text

# Register the UDF
translate_udf = udf(translate_to_english, StringType())

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Drop the target table if it exists
spark.sql("DROP TABLE IF EXISTS purgo_playground.translated_other_language")

# Load the data from the 'purgo_playground.other_language' table
df = spark.table("purgo_playground.other_language")

# Apply the translation UDF to the 'text' column
translated_df = df.withColumn("translated_text", translate_udf(col("text")))

# Save the translated DataFrame to the target table with mergeSchema option set to true
translated_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("purgo_playground.translated_other_language")

