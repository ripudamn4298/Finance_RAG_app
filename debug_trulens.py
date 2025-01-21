from trulens.apps.custom import instrument
from trulens.core import TruSession
from trulens.providers.cortex.provider import Cortex
from trulens.core import Feedback
from trulens.core import Select
from trulens.connectors.snowflake import SnowflakeConnector
import numpy as np
from snowflake.snowpark.session import Session
from dotenv import load_dotenv
import os

# Simple class with basic functions to test instrumentation
class TestRAG:
    def __init__(self):
        print("Initializing TestRAG")
        
    @instrument
    def test_retrieve(self, query: str) -> str:
        print(f"Inside test_retrieve with query: {query}")
        return f"Retrieved context for: {query}"
        
    @instrument
    def test_generate(self, query: str) -> str:
        print(f"Inside test_generate with query: {query}")
        context = self.test_retrieve(query)
        return f"Generated response using {context}"

def setup_test_feedback(provider):
    print("\nSetting up test feedback...")
    
    # Modified test feedback with proper input/output specification
    f_test = (
        Feedback(
            lambda query, response: 1.0,  # Lambda with both input and output parameters
            name="Test Feedback"
        )
        .on_input()  # Specify we're using the input
        .on(Select.RecordCalls.test_retrieve.rets)  # Specify the method output we want
    )
    print("Created test feedback")

    # Add a simpler feedback just for the output
    f_test2 = (
        Feedback(
            lambda response: 1.0,  # Lambda just for output
            name="Test Output Feedback"
        )
        .on_output()  # Only care about the output
    )
    print("Created output feedback")
    
    return [f_test, f_test2]

def main():
    print("\n=== Starting TruLens Debug Test ===")
    
    # Create Snowflake session
    load_dotenv()
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    
    print("\nCreating Snowflake session...")
    session = Session.builder.configs(connection_parameters).create()
    print("Session created")
    
    try:
        # Initialize components
        print("\nInitializing components...")
        test_app = TestRAG()
        connector = SnowflakeConnector(
            snowpark_session=session,
            password=os.getenv("SNOWFLAKE_USER_PASSWORD")  # Add password explicitly
        )
        tru = TruSession(connector=connector)
        provider = Cortex(session, "mistral-large2")
        
        # Setup feedback
        feedbacks = setup_test_feedback(provider)
        
        # Create TruCustomApp
        print("\nCreating TruCustomApp...")
        from trulens.apps.custom import TruCustomApp
        tru_app = TruCustomApp(test_app, app_id="debug_test", feedbacks=feedbacks)
        print(f"\nIs identify_query_parameters instrumented: {hasattr(test_app.test_retrieve, '_tru_wrapped')}")
        
        # Test queries
        test_queries = [
            "Test query 1",
            "Test query 2"
        ]
        
        print("\nRunning test queries...")
        with tru_app as recording:
            for query in test_queries:
                print(f"\nProcessing: {query}")
                response = test_app.test_generate(query)
                print(f"Got response: {response}")
        
        print("\nGetting leaderboard...")
        leaderboard = tru.get_leaderboard()
        print("\nLeaderboard:")
        print(leaderboard)
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        print("\nTraceback:")
        print(traceback.format_exc())
    finally:
        print("\nClosing session...")
        session.close()
        
    print("\n=== Debug Test Complete ===")

if __name__ == "__main__":
    main()