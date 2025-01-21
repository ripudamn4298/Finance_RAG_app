from dotenv import load_dotenv
from snowflake.snowpark.session import Session
import os
from financial_system.financial_advisor_rag import FinancialAdvisorRAG
import pandas as pd
from datetime import datetime
from trulens.apps.custom import TruCustomApp
from trulens.core import TruSession
from trulens.providers.cortex.provider import Cortex
from trulens.core import Feedback
from trulens.core import Select
from trulens.connectors.snowflake import SnowflakeConnector
import numpy as np
from trulens.apps.custom import instrument
import json

def create_session():
    """Create Snowflake session with keep-alive mechanism"""
    load_dotenv()
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "client_session_keep_alive": True
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
        session.sql("SELECT 1").collect()
        print("Successfully connected to Snowflake")
        return session
    except Exception as e:
        print(f"Failed to create Snowflake session: {str(e)}")
        raise

def setup_feedback(provider, tru_session):
    print("Setting up feedbacks...")

    # Groundedness feedback
    def debug_groundedness(response, *args, **kwargs):
        print("\nGroundedness check input:")
        print(f"Response type: {type(response)}")
        print(f"Response preview: {str(response)[:100]}")
        return provider.groundedness_measure_with_cot_reasons(response)

    f_groundedness = (
        Feedback(
            debug_groundedness,
            name="Groundedness"
        )
        .on(Select.RecordCalls.generate_financial_response.rets)
        .on_output()
    )
    
    # Similar debug wrapper for context relevance
    def debug_context_relevance(input_text, response, *args, **kwargs):
        print("\nContext relevance check input:")
        print(f"Input type: {type(input_text)}")
        print(f"Input preview: {str(input_text)[:100]}")
        print(f"Response type: {type(response)}")
        print(f"Response preview: {str(response)[:100]}")
        return provider.context_relevance(input_text, response)

    f_context_relevance = (
        Feedback(
            debug_context_relevance,
            name="Context Relevance"
        )
        .on_input()
        .on(Select.RecordCalls.generate_financial_response.rets)
        .aggregate(np.mean)
    )

    # Answer relevance feedback
    f_answer_relevance = (
        Feedback(
            provider.relevance,
            name="Answer Relevance"
        )
        .on_input()  # Use input query
        .on_output()  # Use final response
        .aggregate(np.mean)
    )
    print("Created answer relevance feedback")

    # Financial accuracy feedback
    def financial_accuracy(response):
        try:
            import re
            numbers = re.findall(r'\$?\d+\.?\d*[BMK]?', response)
            financial_terms = ['revenue', 'profit', 'margin', 'growth', 'eps', 'pe ratio']
            term_count = sum(1 for term in financial_terms if term.lower() in response.lower())
            
            number_score = min(len(numbers) / 3, 1.0)
            term_score = min(term_count / 3, 1.0)
            return (number_score + term_score) / 2
        except Exception:
            return 0.0

    f_financial = (
        Feedback(
            financial_accuracy,  # Single parameter function
            name="Financial Accuracy"
        )
        .on_output()  # Only needs the response
    )
    print("Created financial accuracy feedback")

    # Response time feedback
    def response_time(record):
        try:
            duration = (record.ts_end - record.ts_start).total_seconds()
            return max(0, 1 - (duration / 5))
        except Exception:
            return 0.0

    f_time = (
        Feedback(
            response_time,  # Single parameter function
            name="Response Time"
        )
        .on(Select.RecordCalls.generate_financial_response)  # Track full function execution
    )
    print("Created response time feedback")

    return [f_groundedness, f_context_relevance, f_answer_relevance, f_financial, f_time]

def get_test_queries():
    """Get list of test queries for evaluation"""
    return [
        # General Overview Queries
        "How is Apple performing?",
        # "Tell me about Tesla's current status",
        # "What's the latest on NVIDIA?",
        # "Give me an overview of Microsoft",
        
        # Financial Performance Queries
        "What is Apple's revenue growth?",
        # "Show me Tesla's profit margins",
        # "What are NVIDIA's earnings this quarter?",
        # "How profitable is Microsoft?",
        
        # Valuation Queries
        "What's Apple's current valuation?",
        # "Is Tesla stock overvalued?",
        # "What's NVIDIA's PE ratio?",
        # "How does Microsoft's valuation compare to peers?",
        
        # Market Performance Queries
        "How is Apple stock performing lately?",
        # "What's Tesla's market cap?",
        # "Show me NVIDIA's stock trend",
        # "Has Microsoft stock gone up this year?",
        
        # Specific Financial Metrics
        "What's Apple's operating margin?",
        # "Show me Tesla's debt levels",
        # "What's NVIDIA's ROE?",
        # "What's Microsoft's dividend yield?",
        
        # Comparative Queries
        "How does Apple compare to its sector?",
        # "Is Tesla outperforming other automakers?",
        # "Compare NVIDIA to AMD",
        # "Microsoft vs other tech companies",
        
        # Investment Focused
        "Is Apple a good investment right now?",
        # "Should I invest in Tesla?",
        # "What's the investment outlook for NVIDIA?",
        # "Is Microsoft worth buying?"
    ]

def run_test_queries(session=None):
    """Run test queries and collect metrics"""
    # print("\n=== TruLens Installation Debug ===")
        
    # # Check TruLens packages
    # import pkgutil
    # import trulens
    # trulens_path = trulens.__path__[0]
    # print(f"\nTruLens installation path: {trulens_path}")
    
    # print("\nAvailable TruLens modules:")
    # for module in pkgutil.iter_modules(trulens.__path__):
    #     print(f"- {module.name}")
        
    # # Check specific TruLens components
    # from trulens.apps.custom import instrument
    # import inspect
    # print("\nInstrument decorator details:")
    # print(f"Location: {inspect.getfile(instrument)}")
    # print(f"Source:\n{inspect.getsource(instrument)}")
    
    # # Create advisor
    # advisor = FinancialAdvisorRAG(session)
    
    # # Detailed method inspection
    # print("\nInspecting advisor methods:")
    # methods_to_check = ['generate_financial_response', 'identify_query_parameters', 
    #                     'process_query', 'answer_quick_query']
    
    # for method_name in methods_to_check:
    #     method = getattr(advisor, method_name)
    #     print(f"\nMethod: {method_name}")
    #     print(f"Type: {type(method)}")
    #     print(f"Is callable: {callable(method)}")
    #     print(f"Is instrumented (_tru_wrapped): {hasattr(method, '_tru_wrapped')}")
    #     print(f"Method attributes: {dir(method)}")
        
    # # Check if we can access the wrapped method attributes
    # print("\nTrying to access wrapped method attributes:")
    # for method_name in methods_to_check:
    #     method = getattr(advisor, method_name)
    #     try:
    #         wrapped = getattr(method, '__wrapped__', None)
    #         print(f"\n{method_name}:")
    #         print(f"Has __wrapped__: {wrapped is not None}")
    #         if wrapped:
    #             print(f"Wrapped type: {type(wrapped)}")
    #             print(f"Wrapped attributes: {dir(wrapped)}")
    #     except Exception as e:
    #         print(f"Error accessing {method_name} wrapped attributes: {str(e)}")
    
    # # Initialize TruCustomApp with all debug options
    # print("\nInitializing TruCustomApp:")
    # tru_rag = TruCustomApp(
    #     advisor,
    #     app_id='Financial Advisor RAG Test',
    #     feedbacks=feedbacks,
    #     selectors_check_warning=True,
    #     selectors_nocheck=True,
    #     app_cls=FinancialAdvisorRAG  # Add this to help TruLens identify the class
    # )
    

    print("\n=== Starting Test Queries ===")
    
    try:
        # Use provided session or create new one if none provided
        if session is None:
            print("No earlier session found. Creating new Snowflake session...")
            session = create_session()
            
        # Initialize SnowflakeConnector with explicit password
        tru_snowflake_connector = SnowflakeConnector(
            snowpark_session=session,
            password=os.getenv("SNOWFLAKE_USER_PASSWORD")
        )
        
        # Initialize the advisor
        advisor = FinancialAdvisorRAG(session)
        
        # Initialize TruSession with database redaction
        tru_session = TruSession(
            connector=tru_snowflake_connector
            
        )
        
        # Initialize Cortex provider with snowpark session
        provider = Cortex(
            snowpark_session=session,
            model="mistral-large2"
        )
        
        # Setup feedback mechanisms
        feedbacks = setup_feedback(provider, tru_session)
        
        # Create TruCustomApp with feedbacks
        tru_rag = TruCustomApp(
            advisor,
            app_id='Financial Advisor RAG Test',
            feedbacks=feedbacks,
            selectors_check_warning=True,
            selectors_nocheck=True,
            app_cls=FinancialAdvisorRAG
        )
        print("\nVerifying advisor methods:")
        print(f"Methods in advisor: {dir(advisor)}")
        print(f"\nIs generate_financial_response instrumented: {hasattr(advisor.generate_financial_response, '_tru_wrapped')}")
        print(f"\nIs identify_query_parameters instrumented: {hasattr(advisor.identify_query_parameters, '_tru_wrapped')}")

        # Test a single method call
        print("\nTesting method call:")
        test_query = "How is Apple performing?"
        test_params = advisor.identify_query_parameters(test_query)
        if test_params:
            print(f"Identified parameters: {test_params}. Processing query...")
            process_result = advisor.process_query(test_query)
            if process_result.get('status') == 'success':
                print(f"Query processing successful: {process_result['params']}")
                response = advisor.generate_financial_response(test_query, process_result['params'])
                print(f"Test call successful: {response[:100]}...")
        
        # Now check what's recorded
        print("\nChecking advisor record:")
        advisor_methods = [
            method for method in dir(advisor) 
            if callable(getattr(advisor, method)) and not method.startswith('_')
        ]
        print(f"Available methods: {advisor_methods}")

        # Get test queries
        test_queries = get_test_queries()
        print(f"\nPrepared {len(test_queries)} test queries")
        
        # Run queries with metrics collection
        results = []
        with tru_rag as recording:
            for i, query in enumerate(test_queries, 1):
                print(f"\nProcessing query {i}/{len(test_queries)}: {query}")
                try:
                    start_time = datetime.now()
                    
                    # Call process_query directly as it handles parameter identification internally
                    print("\nCalling process_query...")
                    process_result = advisor.process_query(query)
                    
                    print("\nProcess result:")
                    print(json.dumps(process_result, indent=2, default=str))  # Handle datetime serialization
                    
                    if process_result.get('status') == 'success':
                        try:
                            print("\nGenerating financial response...")
                            print("Parameters being passed:")
                            print(json.dumps(process_result['params'], indent=2, default=str))
                            
                            response = advisor.generate_financial_response(
                                query, 
                                process_result['params']
                            )
                            
                            # Record success metrics
                            end_time = datetime.now()
                            duration = (end_time - start_time).total_seconds()
                            
                            result = {
                                'query': query,
                                'success': True,
                                'response_time': duration,
                                'response_length': len(response) if response else 0,
                                'company': process_result['params'].get('symbol'),
                                'query_type': 'specific' if process_result['params'].get('is_specific_query') else 'general',
                                'functions_called': process_result['params'].get('successful_functions', [])
                            }
                            results.append(result)
                            print(f"\n✓ Query processed successfully ({duration:.2f}s)")
                            
                        except Exception as e:
                            print(f"\n❌ Error in response generation:")
                            print(f"Error type: {type(e)}")
                            print(f"Error message: {str(e)}")
                            print("Traceback:")
                            import traceback
                            print(traceback.format_exc())
                            results.append({
                                'query': query,
                                'success': False,
                                'error': f"Response generation: {str(e)}"
                            })
                    else:
                        print(f"\n❌ Query processing failed:")
                        print(f"Status: {process_result.get('status')}")
                        print(f"Message: {process_result.get('message')}")
                        results.append({
                            'query': query,
                            'success': False,
                            'error': process_result.get('message')
                        })
                        
                except Exception as e:
                    print(f"\n❌ Fatal error processing query:")
                    print(f"Error type: {type(e)}")
                    print(f"Error message: {str(e)}")
                    print("Traceback:")
                    import traceback
                    print(traceback.format_exc())
                    results.append({
                        'query': query,
                        'success': False,
                        'error': str(e)
                    })
                    
                print(f"\nQuery {i} processing complete")
                print("="*50)
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs('metrics', exist_ok=True)
        results_df.to_csv(f"metrics/detailed_results_{timestamp}.csv")
        
        return tru_session, results_df
        
    except Exception as e:
        print(f"\n❌ Fatal error in test queries execution:")
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")
        print("Traceback:")
        import traceback
        print(traceback.format_exc())
        raise

def analyze_performance(tru_session=None, results_df=None):
    """Analyze and save TruLens metrics with enhanced analysis"""
    print("\n=== Starting Performance Analysis ===")
    try:
        if tru_session is None or results_df is None:
            print("Running test queries...")
            tru_session, results_df = run_test_queries()
        
        # Get TruLens metrics
        leaderboard = tru_session.get_leaderboard()
        
        # Enhanced analysis
        print("\nDetailed Performance Analysis:")
        print("-----------------------------")
        
        # Overall success rate
        success_rate = results_df['success'].mean() * 100
        print(f"\nOverall Success Rate: {success_rate:.1f}%")
        
        # Response time analysis
        if 'response_time' in results_df.columns:
            print("\nResponse Time Analysis:")
            print(f"Average: {results_df['response_time'].mean():.2f}s")
            print(f"90th percentile: {results_df['response_time'].quantile(0.9):.2f}s")
            print(f"Max: {results_df['response_time'].max():.2f}s")
        
        # Query type analysis
        if 'query_type' in results_df.columns:
            print("\nPerformance by Query Type:")
            by_type = results_df.groupby('query_type')['success'].agg(['count', 'mean'])
            print(by_type)
        
        # Save metrics
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metrics_dir = "metrics"
        os.makedirs(metrics_dir, exist_ok=True)
        
        # Save TruLens metrics
        if isinstance(leaderboard, pd.DataFrame) and not leaderboard.empty:
            leaderboard.to_csv(f"{metrics_dir}/trulens_metrics_{timestamp}.csv")
        
        # Save performance summary
        try:
            with open(f"{metrics_dir}/performance_summary_{timestamp}.txt", 'w') as f:
                f.write("Performance Analysis Summary\n")
                f.write("==========================\n\n")
                
                # Success rates
                success_stats = results_df['success'].value_counts(normalize=True)
                f.write("Success Rates:\n")
                f.write(f"Success: {success_stats.get(True, 0)*100:.1f}%\n")
                f.write(f"Failure: {success_stats.get(False, 0)*100:.1f}%\n\n")
                
                # Response times
                if 'response_time' in results_df.columns:
                    f.write("Response Time Statistics:\n")
                    f.write(f"Average: {results_df['response_time'].mean():.2f}s\n")
                    f.write(f"Median: {results_df['response_time'].median():.2f}s\n")
                    f.write(f"90th percentile: {results_df['response_time'].quantile(0.9):.2f}s\n")
                    f.write(f"Max: {results_df['response_time'].max():.2f}s\n\n")
                
                # Query type breakdown
                if 'query_type' in results_df.columns:
                    f.write("Performance by Query Type:\n")
                    type_stats = results_df.groupby('query_type')['success'].agg(['count', 'mean'])
                    f.write(type_stats.to_string())
                    f.write("\n\n")
                
                # Company breakdown
                if 'company' in results_df.columns:
                    f.write("Performance by Company:\n")
                    company_stats = results_df.groupby('company')['success'].agg(['count', 'mean'])
                    f.write(company_stats.to_string())
                
        except Exception as e:
            print(f"Error generating performance summary: {str(e)}")
        
        print(f"\nMetrics and analysis saved to {metrics_dir}/")
        
    except Exception as e:
        print(f"Error in performance analysis: {str(e)}")

if __name__ == "__main__":
    session = None
    try:
        print("\n=== Starting Financial Advisor RAG Evaluation ===")
        print("\nStep 1: Creating Snowflake session...")
        session = create_session()
        
        print("\nStep 2: Running test queries...")
        tru_session, results_df = run_test_queries(session)
        
        print("\nStep 3: Analyzing performance...")
        analyze_performance(tru_session, results_df)
        
        print("\n=== Evaluation Complete ===")
        print("\nResults have been saved to the metrics directory.")
        
    except Exception as e:
        print(f"\n❌ Fatal error in metrics analysis: {str(e)}")
        import traceback
        print("\nDetailed error traceback:")
        print(traceback.format_exc())
    finally:
        if session:
            try:
                print("\nClosing Snowflake session...")
                session.close()
            except Exception as e:
                print(f"Error closing session: {str(e)}")
                
        print("\nMetrics analysis script completed.")