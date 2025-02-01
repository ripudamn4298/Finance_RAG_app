from datetime import datetime, timedelta
from snowflake.snowpark.session import Session
# from snowflake.cortex import Complete
from .llm_manager import LLMManager
from typing import Tuple, Dict, List
import yfinance as yf
import pandas as pd
from newsapi import NewsApiClient
import os
import json
from .retriever import EnhancedFinancialRetriever 
import requests
import time
from datetime import datetime, timedelta
import numpy as np
from trulens.core import TruSession
from trulens.providers.cortex.provider import Cortex
from trulens.core import Feedback
from trulens.core import Select
from trulens.connectors.snowflake import SnowflakeConnector
from trulens.apps.custom import instrument
import streamlit as st
# from trulens.apps.custom import instrument as trulens_instrument

# def instrument(f):
#     print(f"Instrumenting function: {f.__name__}")
#     return trulens_instrument(f)
# def debug_instrument(f):
#     print(f"Attempting to instrument function: {f.__name__}")
#     instrumented = instrument(f)
#     # The instrumented object doesn't have __name__, so let's just print what we got
#     print(f"Successfully instrumented: {f.__name__} -> {type(instrumented)}")
#     return instrumented
class ChatMemory:
    def __init__(self, max_messages: int = 10):
        self.max_messages = max_messages
        self.messages = []
        self.current_company = None
        
    def add_message(self, role: str, content: str, params: dict = None):
        """Add a new message to the chat history with query parameters"""
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "params": params
        }
        self.messages.append(message)
        
        # Update current company whenever we have valid company params
        if params and params.get('symbol') and params.get('company'):
            self.current_company = {
                'symbol': params.get('symbol'),
                'company': params.get('company')
            }
            print(f"Updated/Maintained current company as: {self.current_company}")
        
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-self.max_messages:]
    
    def get_last_company_params(self) -> dict:
        """Get the most recent company parameters discussed"""
        # First check current company as it's the source of truth
        if self.current_company:
            print(f"Using current tracked company: {self.current_company}")
            return self.current_company
            
        # Fallback: check message history
        for msg in reversed(self.messages):
            if msg.get('params') and msg['params'].get('symbol'):
                company_info = {
                    'symbol': msg['params']['symbol'],
                    'company': msg['params']['company']
                }
                # Update current_company from history
                self.current_company = company_info
                print(f"Restored company from history: {company_info}")
                return company_info
        
        print("No company found in memory or history")
        return None
        
    def get_context(self) -> str:
        """Get formatted chat history for context"""
        context = []
        for msg in self.messages[-3:]:  # Only last 3 messages for concise context
            role = "User" if msg["role"] == "user" else "Assistant"
            context.append(f"{role}: {msg['content']}")
            if self.current_company:  # Always include current company in context
                context.append(f"[Current Company: {self.current_company['company']} ({self.current_company['symbol']})]")
        return "\n".join(context)




class FinancialDataFetcher:
    def __init__(self, session: Session, api_key: str):
        
        
        self.session = session
        self.api_key = api_key
        self.base_url = "https://seeking-alpha.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
        }

    def _write_pandas_without_trulens(self, df, table_name: str, database: str, schema: str):
        """Helper method to write pandas DataFrame without TruLens instrumentation"""
        # Temporarily disable TruLens instrumentation for this specific operation
        original_wrapper = getattr(self.session, '_tru_wrapper', None)
        try:
            if hasattr(self.session, '_tru_wrapper'):
                print("Disabling TruLens instrumentation for write operation")
                delattr(self.session, '_tru_wrapper')
            
            print(f"Writing DataFrame to {database}.{schema}.{table_name}...")
            self.session.write_pandas(
                df,
                table_name,
                database=database,
                schema=schema,
                chunk_size=1000,
                quote_identifiers=False
            )
            print("Write operation completed successfully")
            
        finally:
            # Restore TruLens instrumentation
            if original_wrapper is not None:
                setattr(self.session, '_tru_wrapper', original_wrapper)

    def _needs_update(self, symbol: str, data_type: str, update_threshold: int = 1) -> bool:
        """
        Check if data needs to be updated based on last update timestamp
        """
        # try:
        #     # Inspect the created_at column without TYPEOF
        #     inspect_query = f"""
        #     SELECT 
        #         created_at,
        #         TO_VARCHAR(created_at) as date_string,
        #         COUNT(*) as record_count
        #     FROM fundamental_data.company_summary 
        #     WHERE symbol = '{symbol}'
        #     GROUP BY created_at
        #     ORDER BY created_at DESC
        #     LIMIT 1
        #     """
        #     print(f"\nInspecting created_at column:")
        #     print(f"Query: {inspect_query}")
            
        #     # inspect_result = self.session.sql(inspect_query).collect()
        #     # print("\nInspection Results:")
        #     # if inspect_result:
        #     #     row = inspect_result[0]
        #     #     created_at_value = row['CREATED_AT']
        #     #     print(f"Raw created_at value: {created_at_value}")
        #     #     print(f"Python type: {type(created_at_value)}")
        #     #     print(f"String format: {row['DATE_STRING']}")
        #     #     print(f"Record count: {row['RECORD_COUNT']}")
                
        #     #     # Now we know it's a timestamp, let's handle it properly
        #     #     if isinstance(created_at_value, datetime):
        #     #         current_time = datetime.now()
        #     #         time_since_update = current_time - created_at_value
        #     #         hours_since_update = time_since_update.total_seconds() / 3600
                    
        #     #         print(f"\nTime comparison:")
        #     #         print(f"Last update: {created_at_value}")
        #     #         print(f"Current time: {current_time}")
        #     #         print(f"Hours since update: {hours_since_update:.2f}")
                    
        #     #         return hours_since_update > update_threshold
        #     print("Returning true")    
        #     return True  # Default to update if no valid timestamp found

        # except Exception as e:
        #     print(f"\nError during column inspection:")
        #     print(f"Error type: {type(e)}")
        #     print(f"Error message: {str(e)}")
        #     import traceback
        #     print(f"Traceback:\n{traceback.format_exc()}")
        #     return True

        
        print("\n=== Starting _needs_update check ===")
        print(f"Parameters:")
        print(f"Symbol: {symbol}")
        print(f"Data type: {data_type}")
        print(f"Update threshold: {update_threshold} hours")
        try:
            # Map data types to their respective tables
            print("\nMapping data type to table...")
            table_map = {
                'summary': 'fundamental_data.company_summary',
                'financials': 'fundamental_data.income_statements',
                'fundamentals': 'fundamental_data.fundamental_metrics',
                'valuation': 'fundamental_data.valuation_metrics',
                'dividend': 'fundamental_data.dividend_history',
                'chart': 'market_data.historical_charts',
                'sector': 'sector_data.sector_metrics'
            }
            
            table_name = table_map.get(data_type)
            if not table_name:
                print(f"Warning: Unknown data type {data_type}")
                return True
            
            print(f"Using table: {table_name}")    
            query = f"""
            SELECT 
                MAX(created_at) as last_update,
                COUNT(*) as record_count
            FROM {table_name}
            WHERE symbol = '{symbol}'
            """
            print(f"\nExecuting query: {query}")
            result = self.session.sql(query).collect()
            
            # Convert to dict and handle datetime serialization
            result_dict = {
                'LAST_UPDATE': result[0]['LAST_UPDATE'].isoformat() if result and result[0]['LAST_UPDATE'] else None,
                'RECORD_COUNT': int(result[0]['RECORD_COUNT']) if result else 0
            }
            print(f"Raw result: {result}")
            print(f"Processed result: {result_dict}")
            
            if not result_dict['RECORD_COUNT']:
                print(f"No existing {data_type} data found for {symbol}")
                return True
                
            if result_dict['LAST_UPDATE']:
                last_update = datetime.fromisoformat(result_dict['LAST_UPDATE'])
                current_time = datetime.now()
                time_since_update = current_time - last_update
                hours_since_update = time_since_update.total_seconds() / 3600
                
                print(f"Last update: {last_update}")
                print(f"Current time: {current_time}")
                print(f"Hours since update: {hours_since_update:.2f}")
                
                if hours_since_update > update_threshold:
                    print(f"{data_type} data for {symbol} is stale (last updated {last_update})")
                    return True
                    
                print(f"{data_type} data for {symbol} is up to date (last updated {last_update})")
                return False
            
            return True
                
        except Exception as e:
            print(f"\nTop level error in _needs_update:")
            print(f"Error type: {type(e)}")
            print(f"Error message: {str(e)}")
            import traceback
            print(f"Traceback:\n{traceback.format_exc()}")
            return True

    def _safe_parse_date(self, date_str: str) -> datetime:
        """Safely parse various date formats"""
        # Skip 'Last Report' and 'TTM' dates as they're duplicates of most recent date
        if date_str in ['Last Report', 'TTM']:
            return None
            
        date_formats = [
            '%b %Y',  # Sep 2023
            '%B %Y',  # September 2023
            '%Y-%m-%d'  # 2023-09-30
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
                
        raise ValueError(f"Unable to parse date: {date_str}")
    
    def _safe_float(value, default=0.0):
        """Safely convert value to float, handling None and empty values"""
        if value is None or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _fetch_and_store_summary(self, symbol: str):
        """Fetch and store company summary data"""
        try:
            if not self._needs_update(symbol, 'summary', update_threshold=24):
                print(f"Summary data for {symbol} is current, skipping fetch")
                return
            
            # First convert trading_date (if exists) to string
            # current_date = datetime.now().strftime('%Y-%m-%d')
            # current_timestamp = datetime.now()
            
            print(f"Fetching fresh summary data for {symbol}")
            response = requests.get(
                f"{self.base_url}/symbols/get-summary",
                headers=self.headers,
                params={"symbols": symbol}
            )
            # Add detailed debugging of response
            print("\nAPI Response Debug:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Raw Response Text: {response.text[:500]}")
            if response.status_code == 200:
                try:
                    # Pre-parse debug
                    print("\nAttempting to parse JSON...")
                    try:
                        raw_json = response.json()
                        print("Successfully parsed raw JSON")
                        print(f"Raw JSON structure: {raw_json.keys() if isinstance(raw_json, dict) else 'Not a dict'}")
                    except json.JSONDecodeError as je:
                        print(f"JSON Parse Error: {str(je)}")
                        print(f"Response encoding: {response.encoding}")
                        print(f"Content type: {response.headers.get('content-type')}")
                        raise

                    data = raw_json['data'][0]['attributes']
                    print(f"\nExtracted data attributes: {list(data.keys())}")
                        
                    
                    # Create DataFrame with explicit date string conversions
                    df_summary = pd.DataFrame([{
                        'symbol': symbol,
                        'company_name': data.get('companyName', ''),  # Empty string for missing name
                        'market_cap': self._safe_float(data.get('marketCap')),
                        'pe_ratio_fwd': self._safe_float(data.get('peRatioFwd')),
                        'dividend_yield': self._safe_float(data.get('divYieldFwd')),
                        'total_debt': self._safe_float(data.get('totalDebt')),
                        'cash': self._safe_float(data.get('cash')),
                        'revenue_growth': self._safe_float(data.get('revenueGrowth')),
                        'net_margin': self._safe_float(data.get('netMargin')),
                        'ebit_margin': self._safe_float(data.get('ebitMargin')),
                        'gross_margin': self._safe_float(data.get('grossMargin')),
                        'debt_to_equity': self._safe_float(data.get('debtEq')),
                        'total_assets': self._safe_float(data.get('totLiabTotAssets')),
                        'number_of_employees': self._safe_float(data.get('numberOfEmployees')),
                        'sector': str(data.get('sectorname', '')),
                        'industry': str(data.get('primaryname', '')),
                        'country': str(data.get('country', ''))
                    }])

                    print("Created DataFrame, writing to database...")
                    self.session.write_pandas(
                        df_summary,
                        "company_summary",
                        database="fin_database",
                        schema="fundamental_data",
                        chunk_size=1000,
                        quote_identifiers=False
                    )
                    # self._write_pandas_without_trulens(
                    #     df_summary,
                    #     "company_summary",
                    #     "fin_database",
                    #     "fundamental_data"
                    # )
                    
                    print("Successfully stored summary data")
                    return True
                
                except ValueError as ve:
                    print(f"Error parsing values: {str(ve)}")
                    print(f"Raw data sample: {str(data)[:200]}")
                    raise
                except Exception as e:
                    print(f"Error processing response: {str(e)}")
                    print(f"Response content: {response.text[:200]}")
                    raise
            else:
                print(f"Error: API call failed with status code {response.status_code}")
                print(f"Response: {response.text[:200]}")
                raise Exception(f"API call failed with status {response.status_code}")
        except Exception as e:
            print(f"Error fetching summary data: {str(e)}")
            raise

    def _fetch_and_store_financials(self, symbol: str, period_type: str = 'annual'):
        """
        Fetch and store all financial statements
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period_type: 'annual' or 'quarterly'
        """
        try:
            if not self._needs_update(symbol, 'financials', update_threshold=24):
                print(f"Financials data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh Financials data for {symbol}")
            # Validate period type
            if period_type not in ['annual', 'quarterly']:
                print(f"\nWarning: Invalid period type '{period_type}', defaulting to 'annual'")
                period_type = 'annual'
                
            print(f"\n=== Fetching Financial Statements ===")
            print(f"Symbol: {symbol}")
            print(f"Period Type: {period_type}")
            
            statement_types = {
                "income-statement": "income_statements",
                "balance-sheet": "balance_sheets",
                "cash-flow-statement": "cash_flow_statements"
            }
            
            start_time = datetime.now()
            print(f"Started at: {start_time}")
            
            for statement_type, table_name in statement_types.items():
                statement_start = datetime.now()
                print(f"\nFetching {statement_type}...")
                
                response = requests.get(
                    f"{self.base_url}/symbols/get-financials",
                    headers=self.headers,
                    params={
                        "symbol": symbol,
                        "period_type": period_type,
                        "statement_type": statement_type
                    }
                )
                
                api_time = datetime.now()
                print(f"API call took: {api_time - statement_start}")
                
                if response.status_code == 200:
                    data = response.json()
                    financial_records = []
                    
                    print("Processing statement data...")
                    for section in data:
                        section_title = section.get('title', '').lower()
                        for row in section.get('rows', []):
                            if row.get('cells'):
                                metric_name = row['name']
                                for cell in row['cells']:
                                    if 'raw_value' in cell and cell.get('name'):
                                        try:
                                            report_date = self._safe_parse_date(cell['name'])
                                            if report_date:
                                                record = {
                                                    'symbol': symbol,
                                                    'report_date': report_date,
                                                    'period_type': period_type,
                                                    'metric_name': metric_name,
                                                    'value': cell.get('raw_value'),
                                                    'yoy_change': cell.get('yoy_value'),
                                                    'statement_type': statement_type,
                                                    'section': section_title
                                                    
                                                }
                                                financial_records.append(record)
                                        except ValueError as e:
                                            print(f"Error parsing date {cell['name']}: {str(e)}")
                                            continue
                    
                    processing_time = datetime.now()
                    print(f"Data processing took: {processing_time - api_time}")
                    
                    if financial_records:
                        print(f"\nPreparing {statement_type} DataFrame...")
                        df = pd.DataFrame(financial_records)
                        
                        print("\nDataFrame Info:")
                        print(f"Total records: {len(df)}")
                        
                        df['report_date'] = df['report_date'].dt.date
                        print(f"Date range: {df['report_date'].min()} to {df['report_date'].max()}")

                        
                        print(f"\nWriting to {table_name}...")
                        self.session.write_pandas(
                            df,
                            table_name,
                            database="fin_database",
                            schema="fundamental_data",
                            chunk_size=1000,
                            quote_identifiers=False
                        )
                        
                        write_time = datetime.now()
                        print(f"Database write took: {write_time - processing_time}")
                        print(f"Successfully stored {len(financial_records)} {statement_type} records")
                    else:
                        print(f"No valid records found for {statement_type}")
                else:
                    print(f"Error: API call failed with status code {response.status_code}")
                    print(f"Response: {response.text[:500]}...")
            
            end_time = datetime.now()
            print(f"\nTotal execution time: {end_time - start_time}")
            
        except requests.exceptions.RequestException as e:
            print(f"Network error during financial data fetch: {str(e)}")
            raise
        except Exception as e:
            print(f"Error in financial data processing: {str(e)}")
            print(f"Error type: {type(e)}")
            raise
            
        print("\n=== Financial Statements Fetch Complete ===")

    def _fetch_and_store_fundamentals(self, symbol: str, period_type: str = 'annual'):
        """
        Fetch and store fundamental metrics
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period_type: 'annual' or 'quarterly'
        """
        try:
            if not self._needs_update(symbol, 'fundamentals',update_threshold=24):
                print(f"Fundamentals data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh Fundamentals data for {symbol}")
            # Validate period type
            if period_type not in ['annual', 'quarterly', 'ttm']:
                print(f"\nWarning: Invalid period type '{period_type}', defaulting to 'annual'")
                period_type = 'annual'
                
            print(f"\n=== Fetching Fundamental Metrics ===")
            print(f"Symbol: {symbol}")
            print(f"Period Type: {period_type}")
            
            # Define important fundamental metrics to fetch
            metrics = [
                "revenues", "total_revenue", "cost_revenue", "gross_profit",
                "operating_income", "net_income", "eps", "diluted_eps",
                "div_rate", "payout_ratio", "ebitda", "profit_margin",
                "roa", "roe", "total_assets", "total_debt",
                "revenue_growth", "cash_flow_per_share"
            ]
            
            start_time = datetime.now()
            print(f"Started at: {start_time}")
            print(f"Fetching {len(metrics)} fundamental metrics...")
            
            all_fundamental_records = []
            metrics_processed = 0
            failed_metrics = []

            for metric in metrics:
                metric_start = datetime.now()
                print(f"\nFetching {metric}...")
                
                try:
                    response = requests.get(
                        f"{self.base_url}/symbols/get-fundamentals",
                        headers=self.headers,
                        params={
                            "symbol": symbol,
                            "limit": "10",  # Last 10 periods
                            "period_type": period_type,
                            "field": metric
                        }
                    )
                    
                    api_time = datetime.now()
                    print(f"API call took: {api_time - metric_start}")
                    
                    if response.status_code == 200:
                        data = response.json().get('data', [])
                        for item in data:
                            try:
                                attributes = item.get('attributes', {})
                                meta = item.get('meta', {})
                                
                                date_str = attributes.get('period_end_date', '').split('T')[0]
                                metric_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                
                                record = {
                                    'symbol': symbol,
                                    'metric_date': metric_date,
                                    'metric_name': metric,
                                    'metric_value': attributes.get('value'),
                                    'year': attributes.get('year'),
                                    'quarter': attributes.get('quarter'),
                                    'is_fiscal': meta.get('is_fiscal', False),
                                    'period_type': period_type
                                }
                                all_fundamental_records.append(record)
                                metrics_processed += 1
                                
                            except (ValueError, KeyError) as e:
                                print(f"Error processing record for {metric}: {str(e)}")
                                continue
                    else:
                        failed_metrics.append(metric)
                        print(f"Failed to fetch {metric}: {response.status_code}")
                        print(f"Response: {response.text[:500]}...")
                        
                except Exception as e:
                    failed_metrics.append(metric)
                    print(f"Error fetching {metric}: {str(e)}")
                    continue
                    
                # Add a small delay to avoid rate limiting
                time.sleep(0.1)
            
            processing_time = datetime.now()
            print(f"\nMetrics processing summary:")
            print(f"Total metrics attempted: {len(metrics)}")
            print(f"Successful metrics: {metrics_processed}")
            print(f"Failed metrics: {len(failed_metrics)}")
            if failed_metrics:
                print("Failed metrics list:", failed_metrics)
            
            if all_fundamental_records:
                print(f"\nPreparing fundamentals DataFrame...")
                df = pd.DataFrame(all_fundamental_records)
                
                print("\nDataFrame Info:")
                print(f"Total records: {len(df)}")
                print(f"Date range: {df['metric_date'].min()} to {df['metric_date'].max()}")
                print("\nColumn Types:")
                print(df.dtypes)
                
                print("\nWriting to database...")
                self.session.write_pandas(
                    df,
                    "fundamental_metrics",
                    database="fin_database",
                    schema="fundamental_data",
                    chunk_size=1000,
                    quote_identifiers=False
                )
                
                end_time = datetime.now()
                print(f"\nDatabase write took: {end_time - processing_time}")
                print(f"Total execution time: {end_time - start_time}")
                print(f"Successfully stored {len(all_fundamental_records)} fundamental records")
            else:
                print("No valid fundamental records to store")
                
        except requests.exceptions.RequestException as e:
            print(f"Network error during fundamentals fetch: {str(e)}")
            raise
        except Exception as e:
            print(f"Error in fundamentals processing: {str(e)}")
            print(f"Error type: {type(e)}")
            raise
            
        print("\n=== Fundamental Metrics Fetch Complete ===")
        """Fetch and store fundamental metrics with timing"""
            
            

    def _fetch_and_store_dividend_history(self, symbol: str, params: dict):
        """Fetch and store dividend history data"""
        try:
            if not self._needs_update(symbol, 'dividend',update_threshold=8000):
                print(f"Dividend data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh dividend data for {symbol}")
            start_time = datetime.now()
            print(f"\nStarting dividend history fetch at: {start_time}")

            # Calculate years between start and end date
            start_date = datetime.strptime(params['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(params['end_date'], '%Y-%m-%d')
            years = (end_date - start_date).days / 365
            years = max(min(round(years), 10), 1)  # Limit between 1 and 10 years
            
            print(f"Calculated years to fetch: {years}")

            # Analyze query for grouping preference
            group_by_prompt = f"""
            Analyze this query to determine if the user wants dividend data grouped by year or month:
            Query: {params['original_query']}
            
            Return ONLY 'year' or 'month'. Default to 'year' if unclear.
            """
            
            print("\nAnalyzing grouping preference...")
            prompt_start = datetime.now()
            # group_by = Complete("mistral-large2", group_by_prompt).strip().lower()
            group_by = self.llm(group_by_prompt)
            prompt_end = datetime.now()
            print(f"Group by analysis took: {prompt_end - prompt_start}")
            print(f"Determined group_by: {group_by}")
            
            # Validate and default if needed
            if group_by not in ['year', 'month']:
                group_by = 'year'

            print(f"\nFetching dividend data with parameters:")
            print(f"Years: {years}")
            print(f"Group by: {group_by}")
            
            api_start = datetime.now()
            response = requests.get(
                f"{self.base_url}/symbols/get-dividend-history",
                headers=self.headers,
                params={
                    "symbol": symbol,
                    "years": str(years),
                    "group_by": group_by
                }
            )
            api_end = datetime.now()
            print(f"API call took: {api_end - api_start}")
            
            
            if response.status_code == 200:
                data = response.json().get('data', [])
                dividend_records = []
                
                for item in data:
                    attributes = item.get('attributes', {})
                    record = {
                        'symbol': symbol,
                        'year': attributes.get('year'),
                        'adjusted_amount': attributes.get('adjusted_amount'),
                        'split_adj_factor': attributes.get('split_adj_factor'),
                        'dividend_type': attributes.get('type')
                    }
                    dividend_records.append(record)
                
                if dividend_records:
                    df = pd.DataFrame(dividend_records)
                    self.session.write_pandas(
                        df,
                        "dividend_history",
                        database="fin_database",
                        schema="fundamental_data",
                        chunk_size=1000,
                        quote_identifiers=False
                    )
                    print(f"Successfully stored {len(dividend_records)} dividend records")
                    
        except Exception as e:
            print(f"Error fetching dividend history: {str(e)}")
            raise

    def _fetch_and_store_chart_data(self, symbol: str, time_period: str = '1Y'):
        """
        Fetch and store historical chart data
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            time_period: One of '1D', '5D', '1M', '6M', 'YTD', '1Y', '3Y', '5Y', '10Y', 'MAX'
        """
        try:
            if not self._needs_update(symbol, 'chart',update_threshold=24):
                print(f"Chart data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh chart data for {symbol}")
            # Validate time period
            allowed_periods = ['1D', '5D', '1M', '6M', 'YTD', '1Y', '3Y', '5Y', '10Y', 'MAX']
            if time_period not in allowed_periods:
                print(f"\nWarning: Invalid time period '{time_period}', defaulting to '1Y'")
                time_period = '1Y'
                
            print(f"\n=== Fetching Chart Data ===")
            print(f"Symbol: {symbol}")
            print(f"Time Period: {time_period}")
            
            # Track timing
            start_time = datetime.now()
            print(f"Started at: {start_time}")
            
            response = requests.get(
                f"{self.base_url}/symbols/get-chart",
                headers=self.headers,
                params={
                    "symbol": symbol,
                    "period": time_period
                }
            )
            
            api_time = datetime.now()
            print(f"API call took: {api_time - start_time}")
            
            if response.status_code == 200:
                data = response.json().get('attributes', {})
                all_chart_records = []
                
                print(f"\nProcessing chart data...")
                for date_str, values in data.items():
                    try:
                        # Parse date from the timestamp
                        trading_date = date_str.split()[0]  # Get '2025-01-10' from '2025-01-10 00:00:00'
                        trading_date = datetime.strptime(trading_date, '%Y-%m-%d').date()
                        
                        record = {
                            'symbol': symbol,
                            'trading_date': trading_date,
                            'close': values.get('close'),
                            'high': values.get('high'),
                            'low': values.get('low'),
                            'volume': values.get('volume'),
                            'open': values.get('open'),
                            'period_type': time_period
                        }
                        all_chart_records.append(record)
                        
                    except ValueError as e:
                        print(f"Error parsing date {date_str}: {str(e)}")
                        continue
                
                processing_time = datetime.now()
                print(f"Data processing took: {processing_time - api_time}")
                
                if all_chart_records:
                    print(f"\nPreparing DataFrame...")
                    df = pd.DataFrame(all_chart_records)
                    
                    # Remove duplicates and reset index
                    df = df.drop_duplicates(subset=['symbol', 'trading_date'])
                    df = df.reset_index(drop=True)
                    
                    print("\nDataFrame Info:")
                    print(f"Total records: {len(df)}")
                    print(f"Date range: {df['trading_date'].min()} to {df['trading_date'].max()}")
                    print("\nColumn Types:")
                    print(df.dtypes)
                    
                    print("\nWriting to database...")
                    self.session.write_pandas(
                        df,
                        "historical_charts",
                        database="fin_database",
                        schema="market_data",
                        chunk_size=1000,
                        quote_identifiers=False
                    )
                    
                    end_time = datetime.now()
                    print(f"\nDatabase write took: {end_time - processing_time}")
                    print(f"Total execution time: {end_time - start_time}")
                    print(f"Successfully stored {len(all_chart_records)} chart records")
                else:
                    print("No valid chart records to store")
                    
            else:
                print(f"Error: API call failed with status code {response.status_code}")
                print(f"Response: {response.text[:500]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"Network error during chart data fetch: {str(e)}")
            raise
        except Exception as e:
            print(f"Error in chart data processing: {str(e)}")
            print(f"Error type: {type(e)}")
            raise
            
        print("\n=== Chart Data Fetch Complete ===")

    def _fetch_and_store_valuation(self, symbol: str):
        """
        Fetch and store valuation metrics for a symbol.
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
        """
        try:
            if not self._needs_update(symbol, 'valuation',update_threshold=24):
                print(f"Valuation data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh valuation data for {symbol}")
            print(f"\n=== Fetching Valuation Metrics ===")
            print(f"Symbol: {symbol}")
            
            start_time = datetime.now()
            print(f"Started at: {start_time}")
            
            # First get the latest trading date from price data
            trading_date_query = f"""
            SELECT MAX(trading_date) as last_trade_date
            FROM market_data.historical_charts
            WHERE symbol = '{symbol}'
            """
            result = self.session.sql(trading_date_query).collect()
            trading_date = result[0]['LAST_TRADE_DATE'] if result else datetime.now().date()
            
            print(f"Using valuation date: {trading_date}")
            
            # Fetch valuation data
            response = requests.get(
                f"{self.base_url}/symbols/get-valuation",
                headers=self.headers,
                params={"symbols": symbol}
            )
            
            api_time = datetime.now()
            print(f"API call took: {api_time - start_time}")
            
            if response.status_code == 200:
                data = response.json().get('data', [])
                valuation_records = []
                
                print("Processing valuation data...")
                for item in data:
                    if item.get('id') == symbol:
                        attributes = item.get('attributes', {})
                        
                        record = {
                            'symbol': symbol,
                            'valuation_date': trading_date,  # Using last trading date
                            # Add null checks for each metric with a default value of None
                            'market_cap': attributes.get('marketCap') if attributes.get('marketCap') is not None else None,
                            'total_enterprise': attributes.get('totalEnterprise') if attributes.get('totalEnterprise') is not None else None,
                            'price_earnings_ratio': attributes.get('lastClosePriceEarningsRatio') if attributes.get('lastClosePriceEarningsRatio') is not None else None,
                            'price_cash_flow': attributes.get('priceCf') if attributes.get('priceCf') is not None else None,
                            'price_sales': attributes.get('priceSales') if attributes.get('priceSales') is not None else None,
                            'price_book': attributes.get('priceBook') if attributes.get('priceBook') is not None else None,
                            'ev_ebitda': attributes.get('evEbitda') if attributes.get('evEbitda') is not None else None,
                            'ev_sales': attributes.get('evSales') if attributes.get('evSales') is not None else None,
                            'ev_fcf': attributes.get('evFcf') if attributes.get('evFcf') is not None else None,
                            'pe_ratio_fwd': attributes.get('peRatioFwd') if attributes.get('peRatioFwd') is not None else None,
                            'peg_ratio': attributes.get('pegRatio') if attributes.get('pegRatio') is not None else None,  # Handle null pegRatio
                            'peg_nongaap_fy1': attributes.get('pegNongaapFy1') if attributes.get('pegNongaapFy1') is not None else None,
                            'pe_gaap_fy1': attributes.get('peGaapFy1') if attributes.get('peGaapFy1') is not None else None,
                            'ev_ebitda_fy1': attributes.get('evEbitdaFy1') if attributes.get('evEbitdaFy1') is not None else None,
                            'ev_sales_fy1': attributes.get('evSalesFy1') if attributes.get('evSalesFy1') is not None else None
                        }
                        valuation_records.append(record)
                
                processing_time = datetime.now()
                print(f"Data processing took: {processing_time - api_time}")
                
                if valuation_records:
                    print("\nPreparing valuation DataFrame...")
                    df = pd.DataFrame(valuation_records)
                    
                    # Print key metrics for verification
                    print("\nKey Metrics Preview:")
                    for col in df.columns:
                        if col not in ['symbol', 'metric_date', 'created_at']:
                            value = df[col].iloc[0]
                            # Add null check before formatting
                            if pd.isna(value) or value is None:
                                print(f"{col:20}: None")
                            else:
                                print(f"{col:20}: {value:,.2f}")
                    
                    print("\nWriting to database...")
                    self.session.write_pandas(
                        df,
                        "valuation_metrics",
                        database="fin_database",
                        schema="fundamental_data",
                        chunk_size=1000,
                        quote_identifiers=False
                    )
                    
                    end_time = datetime.now()
                    print(f"\nDatabase write took: {end_time - processing_time}")
                    print(f"Total execution time: {end_time - start_time}")
                    print(f"Successfully stored valuation metrics for {symbol}")
                    
                else:
                    print(f"No valuation data found for {symbol}")
                    
            else:
                print(f"Error: API call failed with status code {response.status_code}")
                print(f"Response: {response.text[:500]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"Network error during valuation fetch: {str(e)}")
            raise
        except Exception as e:
            print(f"Error in valuation processing: {str(e)}")
            print(f"Error type: {type(e)}")
            raise
            
        print("\n=== Valuation Metrics Fetch Complete ===")
    
    def _fetch_and_store_sector_metrics(self, symbol: str):
        """Fetch and store sector metrics data"""
        try:
            if not self._needs_update(symbol, 'sector',update_threshold=72):
                print(f"Sector data for {symbol} is current, skipping fetch")
                return
                
            print(f"Fetching fresh sector data for {symbol}")
            # Define important fields to fetch
            fields = [
                "gross_margin", "ebit_margin", "ebitda_margin", "net_margin",
                "levered_fcf_margin", "rtn_on_common_equity", "return_on_total_capital",
                "return_on_avg_tot_assets", "capex_to_sales", "assets_turnover",
                "cash_from_operations_as_reported", "net_inc_per_employee"
            ]
            
            response = requests.get(
                f"{self.base_url}/symbols/get-sector-metrics",
                headers=self.headers,
                params={
                    "symbol": symbol,
                    "fields": ",".join(fields)
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                metric_records = []
                metric_types = {item['id']: item['attributes']['field'] 
                            for item in data.get('included', []) 
                            if item['type'] == 'metric_type'}
                
                for item in data.get('data', []):
                    attributes = item.get('attributes', {})
                    relationships = item.get('relationships', {})
                    
                    metric_type_id = relationships.get('metric_type', {}).get('data', {}).get('id')
                    sector_data = relationships.get('sector', {}).get('data', {})
                    
                    record = {
                        'symbol': symbol,
                        'metric_type': metric_types.get(metric_type_id),
                        'value': attributes.get('value'),
                        'sector_id': sector_data.get('id'),
                        'sector_type': sector_data.get('type')
                    }
                    metric_records.append(record)
                
                if metric_records:
                    df = pd.DataFrame(metric_records)
                    self.session.write_pandas(
                        df,
                        "sector_metrics",
                        database="fin_database",
                        schema="sector_data",
                        chunk_size=1000,
                        quote_identifiers=False
                    )
                    print(f"Successfully stored {len(metric_records)} sector metric records")
                    
        except Exception as e:
            print(f"Error fetching sector metrics: {str(e)}")
            raise

    def fetch_all_data(self, symbol: str, params: dict):
        """
        Fetch and store all financial data for a symbol based on provided parameters.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            params: Dictionary containing query parameters including:
                - time_period: One of '1D', '5D', '1M', '6M', 'YTD', '1Y', '3Y', '5Y', '10Y', 'MAX'
                - period_type: 'annual' or 'quarterly'
                - start_date: Start date in YYYY-MM-DD format
                - end_date: End date in YYYY-MM-DD format
                - original_query: Original user query
        """
        try:
            fetch_start = datetime.now()
            print("\n========================================")
            print(f"Starting Comprehensive Data Fetch for {symbol}")
            print("========================================")
            print(f"\nFetch started at: {fetch_start}")
            print("\nUsing parameters:")
            print(json.dumps(params, indent=2))
            
            # Dictionary to track timing of each operation
            timing_data = {}
            success_status = {
                "summary": False,
                "financials": False,
                "fundamentals": False,
                "dividends": False,
                "chart": False,
                "sector": False,
                "valuation": False
            }

            try:
                print("\n1. Fetching Company Summary...")
                summary_start = datetime.now()
                self._fetch_and_store_summary(symbol)
                timing_data["summary"] = datetime.now() - summary_start
                success_status["summary"] = True
            except Exception as e:
                print(f"Error in summary fetch: {str(e)}")

            try:
                print("\n2. Fetching Financial Statements...")
                financials_start = datetime.now()
                self._fetch_and_store_financials(symbol, params.get('period_type', 'annual'))
                timing_data["financials"] = datetime.now() - financials_start
                success_status["financials"] = True
            except Exception as e:
                print(f"Error in financials fetch: {str(e)}")

            try:
                print("\n3. Fetching Fundamental Metrics...")
                fundamentals_start = datetime.now()
                self._fetch_and_store_fundamentals(symbol, params.get('period_type', 'annual'))
                timing_data["fundamentals"] = datetime.now() - fundamentals_start
                success_status["fundamentals"] = True
            except Exception as e:
                print(f"Error in fundamentals fetch: {str(e)}")

            try:
                print("\n4. Fetching Dividend History...")
                dividends_start = datetime.now()
                self._fetch_and_store_dividend_history(symbol, params)
                timing_data["dividends"] = datetime.now() - dividends_start
                success_status["dividends"] = True
            except Exception as e:
                print(f"Error in dividend history fetch: {str(e)}")

            try:
                print("\n5. Fetching Chart Data...")
                chart_start = datetime.now()
                self._fetch_and_store_chart_data(symbol, params.get('time_period', '1Y'))
                timing_data["chart"] = datetime.now() - chart_start
                success_status["chart"] = True
            except Exception as e:
                print(f"Error in chart data fetch: {str(e)}")

            try:
                print("\n6. Fetching Sector Metrics...")
                sector_start = datetime.now()
                self._fetch_and_store_sector_metrics(symbol)
                timing_data["sector"] = datetime.now() - sector_start
                success_status["sector"] = True
            except Exception as e:
                print(f"Error in sector metrics fetch: {str(e)}")

            try:
                print("\n7. Fetching Valuation Metrics...")
                valuation_start = datetime.now()
                self._fetch_and_store_valuation(symbol)
                timing_data["valuation"] = datetime.now() - valuation_start
                success_status["valuation"] = True
            except Exception as e:
                print(f"Error in valuation metrics fetch: {str(e)}")

            # Calculate total time
            fetch_end = datetime.now()
            total_time = fetch_end - fetch_start

            # Print summary report
            print("\n========================================")
            print("Data Fetch Summary Report")
            print("========================================")
            print(f"\nTotal Execution Time: {total_time}")
            print("\nTiming Breakdown:")
            for operation, duration in timing_data.items():
                print(f"{operation.capitalize():12}: {duration}")
            
            print("\nSuccess Status:")
            for operation, status in success_status.items():
                status_symbol = "✓" if status else "✗"
                print(f"{operation.capitalize():12}: {status_symbol}")
            
            success_rate = (sum(1 for status in success_status.values() if status) / len(success_status)) * 100
            print(f"\nOverall Success Rate: {success_rate:.1f}%")
            
            print("\n========================================")
            print("Data Fetch Complete")
            print("========================================")
            
            return {
                "success_rate": success_rate,
                "timing_data": {k: str(v) for k, v in timing_data.items()},
                "total_time": str(total_time),
                "success_status": success_status
            }
            
        except Exception as e:
            print(f"\nCritical error in fetch_all_data: {str(e)}")
            print(f"Error type: {type(e)}")
            raise

class FinancialDataManager:
    def __init__(self, session: Session):
        self.session = session
        self.data_fetcher = FinancialDataFetcher(
            session=session,
            # api_key=os.getenv('RAPIDAPI_KEY')
            api_key= st.secrets["rapidapi"]["key"]
        )

    def sync_company_data(self, symbol: str, params: dict):
        """Synchronize all financial data for a company"""
        # First check if we need to update
        if self._needs_update(symbol):
            print(f"Data needs update for {symbol}, fetching fresh data...")
            self.data_fetcher.fetch_all_data(symbol,params)
        else:
            print(f"Using existing data for {symbol}")
            
    
        
class FinancialAdvisorRAG:
    def __init__(self, snowpark_session: Session):
        # from trulens.apps.custom import instrument
        # self.tru_snowflake_connector = SnowflakeConnector(snowpark_session=snowpark_session)
        # self.tru_session = TruSession(connector=self.tru_snowflake_connector)
        # self.provider = Cortex(snowpark_session, "mistral-large2")
        self.chat_memory = ChatMemory(max_messages=10)
        self.session = snowpark_session
        self.data_manager = FinancialDataManager(session=snowpark_session)
        self.retriever = EnhancedFinancialRetriever(snowpark_session)
        self.llm= LLMManager()

    @instrument
    def retrieve(self, query: str, symbol: str = None, params: dict = None) -> dict:
        return self.retriever.retrieve(query, symbol, params)
    
    @instrument
    def identify_query_parameters(self, query: str) -> dict:
        """
        Identify query parameters and required data fetching functions.
        Returns dictionary with query parameters and required functions.
        """
        print("\n=== Starting Query Parameter Identification ===")
        print(f"Original query: {query}")
        
        current_date = datetime.now()
        
        prompt = f"""
        As a financial analyst, carefully analyze this query to determine minimal required data.
        
        Query: "{query}"
        
        Previous context:
        {self.chat_memory.get_context() if hasattr(self, 'chat_memory') else 'None'}

        VERY IMPORTANT GUIDELINES FOR DATA FUNCTION SELECTION:
        1. For general performance queries, ONLY use fetch_summary initially:
        - "How is Tesla doing?"
        - "Tell me about Apple"
        - "What's the latest on NVIDIA?"
        
        2. Add specific functions ONLY when explicitly requested:
        - Dividend history → fetch_dividend_history
        - Balance sheet details → fetch_financials
        - Historical price charts → fetch_chart_data
        - Sector comparison → fetch_sector_metrics
        - Valuation metrics → fetch_valuation
        
        3. Example mappings: (X is any company)
        - "How is X performing?" → [fetch_summary]
        - "Show me X's balance sheet" → [fetch_financials]
        - "What's X's dividend history?" → [fetch_dividend_history]
        - "Compare X to its sector" → [fetch_sector_metrics]
        - "Show me X's revenue growth from financial statements" → [fetch_financials]
        - Can you provide me valuation of X? → [fetch_valuation]
        - Can you tell me cmp of a testla ? -> [fetch_chart_data]
        - What was the performance of X stock over the years? -> [fetch_chart_data]
        - What is the market cap of X? -> [fetch_valuation]
        - What is the revenue of X? -> [fetch_financials]
        - What are the innvestment opportunities in X? -> [fetch_valuation], [fetch_chart_data], [fetch_financials]

        4. Example mappings for period_types:
        - Give me the annual revenue of X -> period_type: annual
        - Cann you instead give me the annual revenue growth of X -> period_type: annual
        - Show me the latest balance sheet numbers of X -> period_type: quarterly
        - What is the quarterly revenue of X -> period_type: quarterly
        - What is the monthly revenue of X -> period_type: monthly
        - What is the TTM revenue of X -> period_type: ttm
        - What is the income statement numbers of X for the last 12 months -> period_type: ttm

        Analyze the query to identify:
        1. Company/stock mentioned (or from context)
        2. Time periods mentioned 
        3. Specific data points requested
        4. MINIMUM functions needed for initial response

        Do not change paramters like start date and end date unless explicitly mentioned by the user. Use time period YTD if the user might be talking about current times like the past week or the past few days or the current market price of a stock. This is today's date-> {current_date.strftime('%Y-%m-%d')}. Use this date and mention that the latest
        
        Return a JSON with EXACTLY these fields:
        {{
            "symbol": "stock symbol (e.g. AAPL)",
            "company_name": "full company name",
            "has_company": boolean,
            "refers_to_previous": boolean,
            "time_period": "1D|5D|1M|6M|YTD|1Y|3Y|5Y|10Y|MAX", # Default to 1Y if not specified
            "period_type": "annual|quarterly|monthly|ttm",
            "end_date": "{current_date.strftime('%Y-%m-%d')}", # this is today's date, i have to specify like this since your training data is outdated. use this date unless specified by the user
            "start_date": "YYYY-MM-DD (IMPORTANT calculate by subtracting time_period from end_date)",
            "required_functions": ["MINIMUM functions needed"],
            "background_functions": ["additional functions to fetch in background for context"],
            "is_specific_query": boolean
        }}

        Return ONLY the JSON object, no other text.
        """

        try:
            # response = Complete("mistral-large2", prompt).strip()
            print("\nPrompting LLM for parameter identification")
            response = self.llm(prompt)
            response = response.replace('```json', '').replace('```', '').strip()
            result = json.loads(response)
            
            print("\nIdentified Parameters:")
            print(json.dumps(result, indent=2))

            # Handle company identification
            if not result.get('has_company'):
                if hasattr(self, 'chat_memory') and self.chat_memory.current_company:
                    result['symbol'] = self.chat_memory.current_company['symbol']
                    result['company_name'] = self.chat_memory.current_company['company']
                    print(f"\nUsing company from memory: {result['symbol']}")
                else:
                    print("\nNo company identified")
                    return None

            # Create final parameters dictionary with background functions
            params = {
                'symbol': result['symbol'],
                'company': result['company_name'],
                'time_period': result.get('time_period', '1Y'),
                'period_type': result.get('period_type', 'annual'),
                'start_date': result.get('start_date'),
                'end_date': current_date.strftime('%Y-%m-%d'),
                'original_query': query,
                'required_functions': result.get('required_functions', ['fetch_summary']),
                'background_functions': result.get('background_functions', []),
                'is_specific_query': result.get('is_specific_query', False)
            }

            print("\nFinal Parameters:")
            print(json.dumps(params, indent=2))
            print("\nRequired Functions:")
            print(params['required_functions'])
            if params['background_functions']:
                print("\nBackground Functions:")
                print(params['background_functions'])
            print("\n=== Parameter Identification Complete ===")
            
            return params

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {str(e)}")
            print(f"Raw response: {response}")
            return None
        except Exception as e:
            print(f"Error in parameter identification: {str(e)}")
            return None

    @instrument
    def process_query(self, user_query: str) -> str:
        """Main processing pipeline with selective function calling"""
        # Step 1: Identify query parameters
        params = self.identify_query_parameters(user_query)
        
        if params is None:
            return "Could you please specify which company you'd like to know about?"

        print("\nGot parameters, moving to function execution")    
        try:
            # Step 2: Get list of required functions
            required_functions = params.get('required_functions', [])
            print(f"\nProcessing query with required functions: {required_functions}")
            # Track which functions were actually called
            successful_functions = []
            # Track whether data was newly fetched
            data_fetched = False
            
            # Map of function names to actual methods
            function_map = {
                'fetch_summary': self.data_manager.data_fetcher._fetch_and_store_summary,
                'fetch_financials': self.data_manager.data_fetcher._fetch_and_store_financials,
                'fetch_fundamentals': self.data_manager.data_fetcher._fetch_and_store_fundamentals,
                'fetch_dividend_history': self.data_manager.data_fetcher._fetch_and_store_dividend_history,
                'fetch_chart_data': self.data_manager.data_fetcher._fetch_and_store_chart_data,
                'fetch_valuation': self.data_manager.data_fetcher._fetch_and_store_valuation,
                'fetch_sector_metrics': self.data_manager.data_fetcher._fetch_and_store_sector_metrics
            }
            
            # Track successful functions
            success_count = 0
            total_functions = len(required_functions)
            
            # Step 3: Execute only required functions
            for func_name in required_functions:
                if func_name in function_map:
                    try:
                        print(f"Found function in map: {func_name}")
                        print(f"Function details: {function_map[func_name]}")
                        
                        # Call the appropriate function with correct parameters
                        # Before function call
                        print(f"About to call {func_name} with symbol: {params['symbol']}")
                        if func_name == 'fetch_financials' or func_name == 'fetch_fundamentals':
                            result = function_map[func_name](params['symbol'], params.get('period_type', 'annual'))
                        elif func_name == 'fetch_dividend_history':
                            result = function_map[func_name](params['symbol'], params)
                        elif func_name == 'fetch_chart_data':
                            result = function_map[func_name](params['symbol'], params.get('time_period', '1Y'))
                        else:
                            result = function_map[func_name](params['symbol'])
                        if result:  # If new data was fetched
                            data_fetched = True
                        successful_functions.append(func_name)
                        print(f"Successfully executed {func_name}")
                        success_count += 1
                        
                    except Exception as e:
                        print(f"\nDetailed error executing {func_name}:")
                        print(f"Error type: {type(e)}")
                        print(f"Error message: {str(e)}")   
                        print("Stack trace:")
                        import traceback
                        print(traceback.format_exc())
                        continue
            
            if data_fetched:
                print("New data fetched, waiting for search index to update...")
                time.sleep(2)  # Wait for 2 seconds to allow indexing
                
                # Verify data is searchable
                max_retries = 3
                for retry in range(max_retries):
                    test_results = self.retriever.retrieve(
                        "test query", 
                        params['symbol'], 
                        {"required_functions": ["fetch_summary"]}
                    )
                    if test_results.get('company_summary'):
                        print("Data verified in search index")
                        break
                    elif retry < max_retries - 1:
                        print(f"Retry {retry + 1}: Waiting for data to be searchable...")
                        time.sleep(2)
                    else:
                        print("Warning: Unable to verify data in search index")
            params['successful_functions'] = successful_functions       
            
            # Step 4: Return success status
            if success_count > 0:
                print("\nGenerating success response")
                return {
                    "status": "success",
                    "message": f"Successfully fetched {success_count}/{total_functions} data sources for {params['company']}",
                    "params": params
                }
            else:
                print("\nGenerating failure response")
                return {
                    "status": "error",
                    "message": f"Failed to fetch data for {params['company']}",
                    "params": params
                }
                
        except Exception as e:
            print(f"\n=== Error in process_query ===")
            print(f"Error type: {type(e)}")
            print(f"Error message: {str(e)}")
            print("Stack trace:")
            import traceback
            print(traceback.format_exc())
            return {
                "status": "error",
                "message": f"Error processing query: {str(e)}",
                "params": params if 'params' in locals() else None
            }
        
    @instrument
    def answer_quick_query(self, user_query: str, params: dict) -> Tuple[bool, str]:
        """
        Attempt to answer query using existing chat history and previously fetched data.
        Returns tuple of (needs_new_data: bool, response: str)
        
        Args:
            user_query: User's question
            params: Identified query parameters including symbol, time period, etc.
        """
        print("\n=== Quick Query Analysis ===")
        print(f"Query: {user_query}")
        print(f"Parameters: {params}")

        # First check if parameters have changed
        last_company = self.chat_memory.get_last_company_params()
        if not last_company or last_company['symbol'] != params['symbol']:
            print("New company detected, needs fresh data fetch")
            return True, None

        # Check if time period has changed
        last_message = next((msg for msg in reversed(self.chat_memory.messages) 
                            if msg['role'] == 'assistant' and msg.get('params')), None)
        
        if last_message and last_message.get('params'):
            last_params = last_message['params']
            if (last_params.get('time_period') != params.get('time_period') or 
                last_params.get('period_type') != params.get('period_type')):
                print("Time period parameters changed, needs fresh data fetch")
                return True, None

        # Get previous responses for context
        prev_context = []
        fetched_functions = set()  # Track which data has been fetched

        print("\nAnalyzing chat history...")
        for msg in self.chat_memory.messages[-5:]:  # Last 5 messages
            if msg['role'] == 'assistant' and msg.get('params'):
                msg_params = msg['params']
                if msg_params.get('symbol') == params['symbol']:
                    prev_context.append(f"Previous response: {msg['content']}")
                    # Track which functions were called for this data
                    if 'required_functions' in msg_params:
                        fetched_functions.update(msg_params['required_functions'])

        print(f"Previously fetched data types: {fetched_functions}")
        prev_context_str = "\n".join(prev_context)

        # Ask LLM if existing data is sufficient
        analysis_prompt = f"""
        Analyze if new data needs to be fetched for this query using the previous context. If the user is asking to format the same data then it is not necessary to fetch new data And if user asks for general information regarding the prevous context then also no data fetching is required
        You are a financial expert assistant. Example: You should know how to calcualte valuation given the metrics and you have access to all the formulae in the financial domain. You should know what data is important to sufficiently answer the user's query. If a user asks if you have some particular data and you don't find it in your chat history then you have to set the fetch_new_data to true and make sure that data is fetched.
        You must respond with a valid JSON object in this exact format:
        {{
            "needs_new_data": true/false,
            "required_functions": []
        }}

        Previous context:
        {prev_context_str}

        New query: {user_query}

        Previously fetched data types: {list(fetched_functions)}

        Consider:
        1. Is the required information already in previous responses?
        2. Are the required data types already fetched?
        3. Is this a clarification question about previous information?

        IMPORTANT: Return ONLY the JSON object, no additional text or explanation.
        """

        try:
            # Add response cleaning and better error handling
            # llm_response = Complete("mistral-large2", analysis_prompt).strip()
            llm_response = self.llm(analysis_prompt)

            print(f"Raw LLM response: {llm_response}")
            
            # Clean the response - remove any non-JSON text
            cleaned_response = llm_response
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response.replace('```json', '').replace('```', '').strip()
            
            # Try to parse JSON
            try:
                analysis = json.loads(cleaned_response)
            except json.JSONDecodeError as je:
                print(f"JSON parsing error: {je}")
                print(f"Attempted to parse: {cleaned_response}")
                # Default to fetching new data on parsing error
                return True, None

            # Validate expected fields
            if not all(key in analysis for key in ['needs_new_data', 'required_functions']):
                print("Missing required fields in analysis response")
                return True, None
                
            print(f"Parsed analysis: {analysis}")

            needs_new_data = analysis['needs_new_data']
            required_functions = set(analysis['required_functions'])

            # Check if we need new data types not previously fetched
            if not needs_new_data and required_functions.issubset(fetched_functions):
                print("Generating response from existing data...")
                # Generate response using existing context
                response_prompt = f"""
                Using only the previous context, provide a direct answer to the query.

                Previous Context:
                {prev_context_str}

                Query: {user_query}

                Guidelines:
                1. Use only information from the context
                2. Be direct and specific
                3. If any required information is missing, indicate that new data fetch is needed
                """

                # response = Complete("mistral-large2", response_prompt)
                response = self.llm(response_prompt)

                return False, response

            print("New data fetch needed")
            print("=== Quick Query Analysis Complete ===")
            return True, None

        except Exception as e:
            print(f"Error in quick query analysis: {str(e)}")
            print("=== Quick Query Analysis Complete ===")
            # On error, default to fetching new data
            return True, None

    @instrument
    def generate_financial_response(self, user_query: str, params: dict) -> str:
        """Generate natural language response using retrieved context"""
        try:
            print("\n=== Starting Financial Response Generation ===")
            print(f"User Query: {user_query}")
            print(f"Parameters: {json.dumps(params, indent=2)}")
            
            symbol = params.get('symbol')
            print(f"Getting context for symbol: {symbol}")
            
            # Get search results from retriever
            context_results = self.retriever.retrieve(user_query,symbol ,params )
            print("\nReceived context results:" ,context_results)
            for service_type, results in context_results.items():
                print(f"\n{service_type} results count: {len(results)}")
                if results:
                    print("First result sample:", results[0].get('chunk', '')[:100] + "...")
            
            # Format context based on query type
            if params.get('is_specific_query'):
                print("\nProcessing specific query...")
                relevant_context = []
                for service_type, results in context_results.items():
                    if service_type in ['company_summary','financial', 'fundamental', 'valuation', 'dividend', 'chart', 'sector']:
                        for result in results:
                            if "chunk" in result:
                                print(f"Adding specific context from {service_type}: {result['chunk'][:100]}...")
                                relevant_context.append(result["chunk"])
            else:
                print("\nProcessing general query...")
                summary_context = []
                for result in context_results.get('company_summary', []):
                    if "chunk" in result:
                        print(f"Adding summary context: {result['chunk'][:100]}...")
                        summary_context.append(result["chunk"])

                relevant_context = summary_context[:2]  # Limit to most relevant summary points
            
            print(f"\nTotal context pieces: {len(relevant_context)}")
            
            # Generate natural language response
            prompt = f"""
            You are a financial analyst assistant. Generate a clear, natural language response to this query using the provided context.
            Example: You should know how to calculate valuation using different metrics and always mention however many ways of calculating something if you are doing so for the user. you have access to all the formulae in the financial domain. You should know what data is important to sufficiently answer the user's query. 
            You know all the financial jargons as the people you will interact with are financial experts. If asked about investment opportunities you should use all the available data from all of our data sources to provide the best possible answer. Use valuation and future prospects of the company to provide a good answer.
            
            Query: {user_query}
            Company: {params['company']} ({symbol})
            
            Available Context:
            {relevant_context}

            Guidelines:
            1. Be concise but informative
            2. Focus on key metrics and trends
            3. Use natural language, avoid technical jargon
            4. If data seems insufficient, mention that you're gathering more information
            5. For general queries, focus on high-level performance
            6. For specific queries, provide detailed analysis of requested metrics
            7. IMPORTANT: Include specific numbers from the context when available
            8. If no specific numbers are found in context, acknowledge the data limitation

            Generate a response that directly answers the query while being engaging and easy to understand.
            If you don't have specific numbers in the context, acknowledge that you're providing a general overview.
            """
            
            print("\nGenerating response using Complete function...")
            # response = Complete("mistral-large2", prompt)
            response = self.llm(prompt)

            print(f"\nGenerated response: {response[:200]}...")
            
            # If this was a general query, add a note about fetching more data
            if not params.get('is_specific_query') and params.get('background_functions'):
                background_note = "\n\nI'm gathering more detailed information in the background. Feel free to ask specific questions about financials, dividends, or other metrics!"
                response += background_note
                print("Added background fetch note")

            print("\n=== Response Generation Complete ===")
            return response

        except Exception as e:
            print(f"Error generating financial response: {str(e)}")
            return "I encountered an error while analyzing the financial data. Please try again."
        
    
        