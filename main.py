# main.py
from dotenv import load_dotenv
from snowflake.snowpark.session import Session
from financial_system import FinancialAdvisorRAG, EnhancedFinancialRetriever
import os

# Load environment variables
load_dotenv()

# Set up Snowflake connection
connection_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
}

# Create Snowflake session
snowpark_session = Session.builder.configs(connection_params).create()

# Initialize the Financial Advisor RAG
advisor = FinancialAdvisorRAG(snowpark_session)

def main():
    # Test the system
    test_query = "What can you tell me about Google's performance in the last two years?"
    print("Processing query:", test_query)
    response = advisor.process_query(test_query)
    print("\nResponse:", response)

if __name__ == "__main__":
    main()