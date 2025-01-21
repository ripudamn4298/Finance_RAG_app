from dotenv import load_dotenv
from snowflake.snowpark.session import Session
import os
from financial_system.financial_advisor_rag import FinancialAdvisorRAG
from trulens.core import TruSession
from trulens.providers.cortex.provider import Cortex
from trulens.core import Feedback
from trulens.core import Select
from trulens.connectors.snowflake import SnowflakeConnector
import numpy as np
from datetime import datetime
import pandas as pd

def create_session():
    """Create basic Snowflake session"""
    load_dotenv()
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
        print("Successfully connected to Snowflake")
        return session
    except Exception as e:
        print(f"Failed to create Snowflake session: {str(e)}")
        raise

def setup_basic_feedback(provider):
    """Setup minimal feedback mechanisms"""
    print("Setting up basic feedback...")
    
    # Simple answer relevance feedback
    f_answer_relevance = (
        Feedback(
            provider.relevance,
            name="Answer Relevance"
        )
        .on_input()
        .on_output()
    )
    
    return [f_answer_relevance]

def run_single_test_query():
    """Run a single test query to verify setup"""
    try:
        print("\n=== Starting Basic Test ===")
        
        # Create session
        print("\nCreating Snowflake session...")
        session = create_session()
        
        # Initialize Snowflake connector
        print("\nInitializing SnowflakeConnector...")
        tru_snowflake_connector = SnowflakeConnector(
            snowpark_session=session
        )
        
        # Initialize TruSession
        print("\nInitializing TruSession...")
        tru_session = TruSession(
            connector=tru_snowflake_connector
        )
        
        # Initialize Cortex
        print("\nInitializing Cortex provider...")
        provider = Cortex(
            snowpark_session=session,
            model="mistral-large2"
        )
        
        # Initialize advisor
        print("\nInitializing Financial Advisor...")
        advisor = FinancialAdvisorRAG(session)
        
        # Setup minimal TruLens
        print("\nSetting up TruLens...")
        feedbacks = setup_basic_feedback(provider)
        
        # Create TruCustomApp
        print("\nCreating TruCustomApp...")
        from trulens.apps.custom import TruCustomApp
        tru_rag = TruCustomApp(
            advisor,
            app_id='Basic RAG Test',
            feedbacks=feedbacks
        )
        
        # Test single query
        test_query = "How is Apple performing?"
        print(f"\nTesting query: {test_query}")
        
        with tru_rag as recording:
            try:
                result = advisor.process_query(test_query)
                print("\nProcess result:", result)
                
                if result.get('status') == 'success':
                    response = advisor.generate_financial_response(
                        test_query,
                        result['params']
                    )
                    print("\nGenerated response:", response[:100])
                    
            except Exception as e:
                print(f"\nError during query processing:")
                print(f"Error type: {type(e)}")
                print(f"Error message: {str(e)}")
                import traceback
                print(traceback.format_exc())
        
        print("\n=== Test Complete ===")
        return tru_session
        
    except Exception as e:
        print(f"\nFatal error in test:")
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    try:
        tru_session = run_single_test_query()
        print("\nRetrieving feedback results...")
        leaderboard = tru_session.get_leaderboard()
        print("\nFeedback results:")
        print(leaderboard)
    except Exception as e:
        print(f"Error in main: {str(e)}")