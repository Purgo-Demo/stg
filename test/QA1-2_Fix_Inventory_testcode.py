from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, length
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.utils import AnalysisException
import unittest
from pyspark.testing.sqlutils import ReusedSQLTestCase

# Initialize Spark session
spark = SparkSession.builder.appName("Databricks Testing").getOrCreate()

class TestDatabricksOperations(ReusedSQLTestCase):

    @classmethod
    def setUpClass(cls):
        cls.employees_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("lastdate", DateType(), True)  # New 'lastdate' field
        ])
        cls.customers_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("categoryGroup", StringType(), True)  # New 'categoryGroup' field
        ])

        cls.employees_data = [
            (1, "John Doe", "2023-01-15"),
            (2, "Jane Smith", "2022-08-30"),
            (3, "Max Mustermann", None),  # NULL lastdate
            (4, "Alice Wonderland", "2025-01-01"),  # Future lastdate
        ]
        cls.customers_data = [
            (1, "Acme Corp", "Premium"),
            (2, "Globex Inc", "Basic"),
            (3, "Soylent Corp", "Enterprise"),
            (4, "Initech", "Uncategorized"),
            (5, "Vandelay Industries", None),  # NULL categoryGroup
            (7, "Oscorp", "123456789012345678901234567890123456789012345678901")  # Length > 50
        ]

        cls.employees_df = spark.createDataFrame(cls.employees_data, schema=cls.employees_schema)
        cls.customers_df = spark.createDataFrame(cls.customers_data, schema=cls.customers_schema)

        cls.employees_df.createOrReplaceTempView("employees_test")
        cls.customers_df.createOrReplaceTempView("customers_test")

    def test_employees_lastdate_default(self):
        result = self.spark.sql("""
        SELECT * FROM employees_test WHERE lastdate IS NULL
        """)
        self.assertEqual(result.count(), 1, "Employees with default 'lastdate' should be NULL")

    def test_employees_lastdate_future_date(self):
        result = self.spark.sql("""
        SELECT * FROM employees_test WHERE lastdate > current_date()
        """)
        self.assertEqual(result.count(), 1, "There should be one employee with a lastdate in the future")

    def test_customers_categoryGroup_default(self):
        result = self.spark.sql("""
        SELECT * FROM customers_test WHERE categoryGroup = 'Uncategorized'
        """)
        self.assertEqual(result.count(), 1, "Customers with default 'categoryGroup' should be 'Uncategorized'")

    def test_customers_categoryGroup_length(self):
        result = self.spark.sql("""
        SELECT * FROM customers_test WHERE length(categoryGroup) > 50
        """)
        self.assertEqual(result.count(), 1, "There should be one customer with 'categoryGroup' length exceeding 50 characters")

    def test_employees_data_type(self):
        try:
            self.spark.sql("ALTER TABLE employees_test ADD COLUMNS (lastdate STRING)").show()
        except AnalysisException as e:
            self.assertIn("Invalid data type for column lastdate", str(e), "Expected data type error for lastdate")

    def test_customers_categoryGroup_not_null(self):
        result = self.spark.sql("""
        SELECT * FROM customers_test WHERE categoryGroup IS NULL
        """)
        self.assertEqual(result.count(), 1, "There should be one customer with NULL 'categoryGroup'")


if __name__ == "__main__":
    unittest.main(verbosity=2)

