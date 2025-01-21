from snowflake.snowpark.session import Session
from snowflake.core import Root
from typing import Dict, List
from trulens.apps.custom import instrument
from datetime import datetime, timedelta
import time

class EnhancedFinancialRetriever:
    def __init__(self, snowpark_session: Session):
        self._session = snowpark_session
        self._root = Root(self._session)
        self._database = self._root.databases["FIN_DATABASE"]
        
        # Define all available search services
        self.services = {
            "company_summary": {
                "name": "COMPANY_SUMMARY_SEARCH_SERVICE",
                "schema": "fundamental_data",
                "limit": 1,  # We only need the latest summary
                "columns": [
                    "chunk", 
                    "symbol", 
                    "company_name", 
                    "sector", 
                    "industry", 
                    "metric_type",
                    "created_at"
                ]
            },
            "financial": {
                "name": "FINANCIAL_STATEMENTS_SEARCH_SERVICE",
                "schema": "fundamental_data",
                "limit": 5,
                "columns": ["chunk", "symbol", "report_date", "statement_type", "section","period_type"]
            },
            "fundamental": {
                "name": "FUNDAMENTAL_SEARCH_SERVICE", 
                "schema": "fundamental_data",
                "limit": 5,
                "columns": ["chunk", "symbol", "metric_date", "period_type"]
            },
            "valuation": {
                "name": "VALUATION_SEARCH_SERVICE",
                "schema": "fundamental_data",
                "limit": 3,
                "columns": ["chunk", "symbol", "valuation_date", "metric_type"]
            },
            "dividend": {
                "name": "DIVIDEND_SEARCH_SERVICE",
                "schema": "fundamental_data",
                "limit": 3,
                "columns": ["chunk", "symbol", "year"]
            },
            "chart": {
                "name": "CHART_SEARCH_SERVICE",
                "schema": "market_data",
                "limit": 5,
                "columns": ["chunk", "symbol", "trading_date"]
            },
            "sector": {
                "name": "SECTOR_METRICS_SEARCH_SERVICE",
                "schema": "sector_data",
                "limit": 3,
                "columns": ["chunk", "symbol", "metric_type", "sector_type"]
            }
        }
    @instrument
    def retrieve(self, query: str, symbol: str = None, params: dict = None) -> Dict[str, List[dict]]:
        """
        Retrieve relevant information based on required functions
        """
        results = {}
        
        if not params or 'required_functions' not in params:
            print("No required functions specified in params")
            return results
        # When specifically looking for summary data, optimize the query
        if 'fetch_summary' in params['required_functions']:
            max_retries = 3
            for retry in range(max_retries):
                try:
                    config = self.services['company_summary']
                    service = (
                        self._database
                        .schemas[config["schema"]]
                        .cortex_search_services[config["name"]]
                    )
                    
                    search_params = {
                        "query": f"{query} {symbol}" if symbol else query,
                        "columns": config["columns"],
                        "limit": config["limit"]
                    }
                    
                    if symbol:
                        search_params["filter"] = {
                            "@eq": {"symbol": symbol}
                        }
                    
                    response = service.search(**search_params)
                    
                    if response.results:
                        print(f"Found {len(response.results)} results on attempt {retry + 1}")
                        results['company_summary'] = response.results
                        break
                    elif retry < max_retries - 1:
                        print(f"No results found on attempt {retry + 1}, retrying...")
                        time.sleep(1)  # Wait 1 second before retry
                    else:
                        print("No results found after all retries")
                        results['company_summary'] = []
                        
                except Exception as e:
                    print(f"Error on attempt {retry + 1}: {str(e)}")
                    if retry == max_retries - 1:
                        results['company_summary'] = []

        if 'fetch_chart_data' in params['required_functions']:
            try:
                config = self.services['chart']
                service = (
                    self._database
                    .schemas[config["schema"]]
                    .cortex_search_services[config["name"]]
                )
                
                search_params = {
                    "query": f"{query} {symbol}" if symbol else query,
                    "columns": config["columns"],
                    "limit": config["limit"],
                    "orderBy": [
                        {"column": "trading_date", "direction": "DESC"}  # Sort by latest date first
                    ]
                }
                
                if symbol:
                    search_params["filter"] = {
                        "@eq": {"symbol": symbol}
                    }
                
                print(f"Searching chart data with params: {search_params}")
                response = service.search(**search_params)
                
                if response.results:
                    # Sort results by date in descending order (latest first)
                    sorted_results = sorted(
                        response.results,
                        key=lambda x: x.get('trading_date', ''),
                        reverse=True
                    )
                    # Take only the most recent result if asking for current price
                    if any(term in query.lower() for term in ['current', 'latest', 'now', 'today', 'price']):
                        results['chart'] = [sorted_results[0]]
                    else:
                        results['chart'] = sorted_results[:config["limit"]]
                else:
                    print("No chart results found")
                    results['chart'] = []
                    
            except Exception as e:
                print(f"Error searching chart data: {str(e)}")
                results['chart'] = []
        
        
        # Map fetch functions to search services
        function_to_service = {
            # 'fetch_summary': ['company_summary'],
            'fetch_financials': ['financial'],
            'fetch_fundamentals': ['fundamental'],
            'fetch_valuation': ['valuation'],
            'fetch_dividend_history': ['dividend'],
            # 'fetch_chart_data': ['chart'],
            'fetch_sector_metrics': ['sector']
        }

        # Get required services based on required functions
        required_services = []
        for func in params['required_functions']:
            if func in function_to_service:
                required_services.extend(function_to_service[func])
        
        print(f"\nQuerying required services: {required_services}")
        
        for service_type in required_services:
            if service_type not in self.services:
                continue
                
            try:
                config = self.services[service_type]
                service = (
                    self._database
                    .schemas[config["schema"]]
                    .cortex_search_services[config["name"]]
                )
                
                search_params = {
                    "query": f"{query} {symbol}" if symbol else query,
                    "columns": config["columns"],
                    "limit": config["limit"]
                }
                
                if symbol:
                    search_params["filter"] = {"@eq": {"symbol": symbol}}
                
                print(f"Searching {service_type} with params: {search_params}")
                response = service.search(**search_params)
                
                if response.results:
                    print(f"Found {len(response.results)} results for {service_type}")
                    results[service_type] = response.results
                else:
                    print(f"No results found for {service_type}")
                    
            except Exception as e:
                print(f"Error searching {service_type}: {str(e)}")
                results[service_type] = []
                
        return results

    def get_combined_context(self, query: str, symbol: str = None, params: dict = None) -> str:
        """
        Get combined context from required services in a formatted string
        """
        results = self.retrieve(query, symbol, params)
        if not results:
            print("No results retrieved")
            return "No relevant financial information found."

        context_parts = []
        total_context_pieces = 0
        
        # Dictionary to map service types to section headers
        headers = {
            "company_summary": "📋 Company Overview",
            "financial": "📊 Financial Statements",
            "fundamental": "📈 Key Metrics",
            "valuation": "💰 Valuation Metrics",
            "dividend": "💸 Dividend Information",
            "chart": "📉 Market Performance",
            "sector": "🏢 Sector Analysis"
        }
        
        # Always process company_summary first if available
        if 'company_summary' in results and results['company_summary']:
            print("Processing company summary")
            context_parts.append(f"\n{headers['company_summary']}:")
            for result in results['company_summary']:
                if isinstance(result, dict) and "chunk" in result:
                    context_parts.append(result["chunk"])
                    total_context_pieces += 1
        
        # Process results from other services
        for service_type, service_results in results.items():
            if service_type != 'company_summary' and service_results:
                print(f"Processing {len(service_results)} results from {service_type}")
                context_parts.append(f"\n{headers.get(service_type, service_type.title())}:")
                
                for result in service_results:
                    if isinstance(result, dict) and "chunk" in result:
                        context_parts.append(result["chunk"])
                        total_context_pieces += 1
                    else:
                        print(f"Skipping malformed result: {result}")
        
        print(f"Total context pieces assembled: {total_context_pieces}")
        
        if not context_parts:
            return "No relevant financial information found."
            
        final_context = "\n".join(context_parts)
        print(f"Final context length: {len(final_context)}")
        return final_context