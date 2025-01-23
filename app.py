def check_conda_environment():
    import os
    import sys
    import streamlit as st
    
    # Get current environment info
    current_env = os.environ.get('CONDA_DEFAULT_ENV')
    
    # For Streamlit Cloud, we should skip the environment check
    if 'STREAMLIT_SHARING' in os.environ:
        return
        
    # For local development, enforce environment
    expected_env = 'getting_started_llmops'
    env_path = '/Users/riparmar/miniconda3/envs/getting_started_llmops'
    
    if current_env != expected_env:
        st.error(f"""
        Wrong Conda environment! 
        Current: {current_env}
        Expected: {expected_env}
        
        Please run:
        conda activate {expected_env}
        """)
        st.stop()
    
    # Check Python interpreter only for local development
    if not sys.executable.startswith(env_path):
        st.error(f"""
        Wrong Python interpreter!
        Current: {sys.executable}
        Expected path should start with: {env_path}
        
        Please ensure you're using the correct Conda environment:
        conda activate {expected_env}
        """)
        st.stop()

# Add this at the start of your app.py, before any other imports
# check_conda_environment()

import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from datetime import datetime, timedelta
from financial_system.financial_advisor_rag import FinancialAdvisorRAG
from financial_system.financial_advisor_rag import ChatMemory
import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Initialize Snowflake session
def create_session():
    try:
        # connection_parameters = {
        #     "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        #     "user": os.getenv("SNOWFLAKE_USER"),
        #     "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
        #     "role": os.getenv("SNOWFLAKE_ROLE"),
        #     "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        #     "database": os.getenv("SNOWFLAKE_DATABASE"),
        #     "schema": os.getenv("SNOWFLAKE_SCHEMA")
        # }
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        
        # Validate required parameters
        missing_params = [k for k, v in connection_parameters.items() if not v]
        if missing_params:
            raise ValueError(f"Missing required Snowflake parameters: {', '.join(missing_params)}")
            
        return Session.builder.configs(connection_parameters).create()
    except Exception as e:
        st.error(f"Failed to create Snowflake session: {str(e)}")
        raise

# Initialize session state
if 'snowpark_session' not in st.session_state:
    st.session_state.snowpark_session = create_session()

def init_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_symbol" not in st.session_state:
        st.session_state.current_symbol = None
    if 'chat_memory' not in st.session_state:
        st.session_state.chat_memory = ChatMemory(max_messages=10)
    if 'financial_advisor' not in st.session_state:
        st.session_state.financial_advisor = FinancialAdvisorRAG(st.session_state.snowpark_session)
        
    if 'fetched_functions' not in st.session_state:

        st.session_state.fetched_functions = {} 
    if 'show_dashboard' not in st.session_state:
        st.session_state.show_dashboard = False
    
    # Always ensure chat memory sync
    st.session_state.financial_advisor.chat_memory = st.session_state.chat_memory

def update_fetched_functions(symbol: str, successful_functions: list):
    """Update the list of successfully fetched functions for a symbol"""
    if symbol not in st.session_state.fetched_functions:
        st.session_state.fetched_functions[symbol] = set()
    st.session_state.fetched_functions[symbol].update(successful_functions)

def show_market_overview(symbol):
    """Display market overview for the current symbol"""
    print(f"Showing market overview for:", {symbol})
    if symbol:
        try:
            # Get company summary
            summary_query = f"""
            SELECT company_name, market_cap, pe_ratio_fwd, dividend_yield, sector
            FROM fundamental_data.company_summary 
            WHERE symbol = '{symbol}'
            ORDER BY created_at DESC
            LIMIT 1
            """
            summary_df = st.session_state.snowpark_session.sql(summary_query).to_pandas()
            
            if not summary_df.empty:
                st.subheader(f"{summary_df['COMPANY_NAME'].iloc[0]} ({symbol})")
                st.caption(f"Sector: {summary_df['SECTOR'].iloc[0]}")
                
                # Show key metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    if 'MARKET_CAP' in summary_df:
                        market_cap_b = summary_df['MARKET_CAP'].iloc[0] / 1e9
                        st.metric("Market Cap", f"${market_cap_b:.2f}B")
                with col2:
                    if 'PE_RATIO_FWD' in summary_df:
                        st.metric("Forward P/E", f"{summary_df['PE_RATIO_FWD'].iloc[0]:.2f}")
                with col3:
                    if 'DIVIDEND_YIELD' in summary_df:
                        st.metric("Dividend Yield", f"{summary_df['DIVIDEND_YIELD'].iloc[0]:.2f}%")

            # Get recent price data
            price_query = f"""
            SELECT 
                trading_date,
                close,
                volume,
                high,
                low,
                open
            FROM market_data.historical_charts
            WHERE symbol = '{symbol}'
            ORDER BY trading_date DESC
            
            """
            price_df = st.session_state.snowpark_session.sql(price_query).to_pandas()
            
            if not price_df.empty:
                st.subheader("Market Performance")
                
                # Price metrics 
                col1, col2, col3 = st.columns(3)
                current_price = price_df['CLOSE'].iloc[0]
                prev_price = price_df['CLOSE'].iloc[1]
                price_change = ((current_price - prev_price) / prev_price) * 100
                
                with col1:
                    st.metric(
                        "Current Price",
                        f"${current_price:.2f}",
                        f"{price_change:.2f}%",
                        delta_color="normal"
                    )
                with col2:
                    st.metric(
                        "24h High",
                        f"${price_df['HIGH'].iloc[0]:.2f}"
                    )
                with col3:
                    st.metric(
                        "24h Low",
                        f"${price_df['LOW'].iloc[0]:.2f}"
                    )
                
                # Volume metrics
                current_vol = price_df['VOLUME'].iloc[0]
                prev_vol = price_df['VOLUME'].iloc[1]
                vol_change = ((current_vol - prev_vol) / prev_vol) * 100
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "Volume",
                        f"{current_vol:,.0f}",
                        f"{vol_change:.2f}%",
                        delta_color="normal"
                    )
                with col2:
                    avg_vol = price_df['VOLUME'].mean()
                    st.metric("Avg Volume (30d)", f"{avg_vol:,.0f}")
                
                # Price and volume charts
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                # Convert to datetime and sort
                price_df['TRADING_DATE'] = pd.to_datetime(price_df['TRADING_DATE'])
                price_df = price_df.sort_values('TRADING_DATE')

                # Create figure with secondary y-axis
                fig = make_subplots(
                    rows=2, cols=1,
                    row_heights=[0.7, 0.3],
                    vertical_spacing=0.05,
                    subplot_titles=("Price", "Volume")
                )

                # Add price line
                fig.add_trace(
                    go.Scatter(
                        x=price_df['TRADING_DATE'],
                        y=price_df['CLOSE'],
                        name="Price",
                        line=dict(color="#1f77b4", width=2)
                    ),
                    row=1, col=1
                )

                # Add volume bars
                fig.add_trace(
                    go.Bar(
                        x=price_df['TRADING_DATE'],
                        y=price_df['VOLUME'],
                        name="Volume",
                        marker_color='rgba(31,119,180,0.3)'
                    ),
                    row=2, col=1
                )

                # Calculate price range for better visualization
                price_min = price_df['CLOSE'].min()
                price_max = price_df['CLOSE'].max()
                price_margin = (price_max - price_min) * 0.1

                # Update layout
                fig.update_layout(
                    height=600,
                    showlegend=False,
                    margin=dict(l=0, r=0, t=30, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis2_showticklabels=True
                )

                # Update y-axes to show grid and set ranges
                fig.update_yaxes(
                    row=1, 
                    showgrid=True,
                    gridcolor='rgba(128,128,128,0.2)',
                    range=[price_min - price_margin, price_max + price_margin],
                    tickformat='$,.2f'
                )
                
                fig.update_yaxes(
                    row=2,
                    showgrid=True,
                    gridcolor='rgba(128,128,128,0.2)',
                    tickformat=',d'
                )

                # Update x-axes
                fig.update_xaxes(
                    showgrid=True,
                    gridcolor='rgba(128,128,128,0.2)',
                    rangeslider_visible=False
                )

                # Display the figure
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
                
            else:
                st.warning(f"No recent market data available for {symbol}")
                
        except Exception as e:
            st.error(f"Error displaying market overview: {str(e)}")
            st.write("Debug info:", {
                "Symbol": symbol,
                "Error type": type(e).__name__,
                "Error details": str(e)
            })

def show_data_status(symbol: str, fetched_functions: set):
    """Display data fetch status for current symbol"""
    # Define all possible functions and their display names
    function_map = {
        'fetch_summary': 'Company Summary',
        'fetch_financials': 'Financial Statements',
        'fetch_fundamentals': 'Fundamental Metrics',
        'fetch_valuation': 'Valuation Metrics',
        'fetch_dividend_history': 'Dividend History',
        'fetch_chart_data': 'Chart Data',
        'fetch_sector_metrics': 'Sector Analysis'
    }
    
    st.subheader(f"Data Status: {symbol}")
    
    # Create two columns for status icons and labels
    for func_name, display_name in function_map.items():
        col1, col2 = st.columns([1, 4])
        with col1:
            if func_name in fetched_functions:
                st.markdown('✅')  # Green checkmark for fetched
            else:
                st.markdown('◯')  # Empty circle for unfetched
        with col2:
            if func_name in fetched_functions:
                st.markdown(f"**{display_name}**")  # Bold for fetched
            else:
                st.markdown(f"{display_name}", help="Not yet fetched")


def generate_dashboard_content():
    # Helper function to generate dashboard HTML
    if not st.session_state.current_symbol:
        return "<p>No company selected</p>"
        
    content = f"""
        <h4>{st.session_state.current_symbol}</h4>
        <ul style="list-style-type: none; padding: 0;">
            <li>✓ Company Summary</li>
            <li>○ Financial Statements</li>
            <li>○ Fundamental Metrics</li>
            <li>○ Valuation Metrics</li>
            <li>○ Dividend History</li>
            <li>✓ Chart Data</li>
            <li>○ Sector Analysis</li>
        </ul>
    """
    return content
def main():
    
    st.title("🤖 Financial Advisor Bot")
    st.write("Ask questions about any company's performance, financials, or news.")
    
    init_session_state()

    # Sidebar for data status
    with st.sidebar:
        st.subheader("Data Fetch Status")
        if st.session_state.current_symbol:
            fetched = st.session_state.fetched_functions.get(
                st.session_state.current_symbol, set()
            )
            show_data_status(st.session_state.current_symbol, fetched)
        else:
            st.info("Select a company to view data fetch status")
    
    # Main content area
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Show market overview if we have a current symbol
    if st.session_state.current_symbol:
        # First check if we have the required data
        check_query = f"""
        SELECT 
            EXISTS (
                SELECT 1 
                FROM market_data.historical_charts 
                WHERE symbol = '{st.session_state.current_symbol}'
            ) as has_chart_data,
            EXISTS (
                SELECT 1 
                FROM fundamental_data.company_summary 
                WHERE symbol = '{st.session_state.current_symbol}'
            ) as has_summary_data
        """
        check_df = st.session_state.snowpark_session.sql(check_query).to_pandas()
        
        if check_df['HAS_CHART_DATA'].iloc[0] and check_df['HAS_SUMMARY_DATA'].iloc[0]:
            show_market_overview(st.session_state.current_symbol)
    
    # Chat input
    if question := st.chat_input("What would you like to know about any company?"):
        
        # Add message to both Streamlit messages and chat memory
        st.session_state.messages.append({"role": "user", "content": question})
        st.session_state.chat_memory.add_message("user", question)
        
        with st.chat_message("user"):
            st.markdown(question)
        
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            
            with st.spinner("Analyzing..."):
                try:
                    # First identify the company and parameters
                    params = st.session_state.financial_advisor.identify_query_parameters(question)
                    
                    if params is None:
                        error_msg = "Could you please specify which company you'd like to know about?"
                        message_placeholder.warning(error_msg)
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": error_msg
                        })
                        st.session_state.chat_memory.add_message("assistant", error_msg)
                        return
                    
                    # Try quick query first
                    needs_new_data, quick_response = st.session_state.financial_advisor.answer_quick_query(question, params)
                    if not needs_new_data and quick_response:
                        # Use quick response
                        message_placeholder.markdown(quick_response)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": quick_response,
                            "params": params
                        })
                    else:
                    
                        # Fetch latest data for the company
                        with st.spinner(f"Updating data for {params['company']}..."):
                            process_result = st.session_state.financial_advisor.process_query(question)
                            
                            if isinstance(process_result, dict) and process_result.get('status') == 'success':
                                # Update fetched functions tracking
                                if 'successful_functions' in process_result['params']:
                                    update_fetched_functions(
                                        params['symbol'], 
                                        process_result['params']['successful_functions']
                                    )
                                with st.spinner("Analyzing financial data..."):
                                    financial_response = st.session_state.financial_advisor.generate_financial_response(
                                        question,
                                        process_result['params']
                                    )
                                    # Update both message displays and chat memory
                                    message_placeholder.markdown(financial_response)
                                    st.session_state.messages.append({
                                        "role": "assistant",
                                        "content": financial_response
                                    })
                                    st.session_state.chat_memory.add_message(
                                        "assistant", 
                                        financial_response, 
                                        process_result['params']
                                    )
                            else:
                                error_msg = process_result.get('message', "Error fetching data. Please try again.")
                                message_placeholder.error(error_msg)
                                st.session_state.chat_memory.add_message("assistant", error_msg)
                                return
                        
                        # Update symbol state if needed
                        if params.get('symbol') and params['symbol'] != st.session_state.current_symbol:
                            st.session_state.current_symbol = params['symbol']
                            st.rerun()
                        
                except Exception as e:
                    error_msg = f"Error processing query: {str(e)}"
                    message_placeholder.error(error_msg)
                    st.error(f"Debug info: {str(e)}")
                    st.session_state.chat_memory.add_message("assistant", error_msg)

# Add session cleanup
def on_shutdown():
    if 'snowpark_session' in st.session_state:
        try:
            st.session_state.snowpark_session.close()
        except:
            pass

import atexit
atexit.register(on_shutdown)

if __name__ == "__main__":
    main()