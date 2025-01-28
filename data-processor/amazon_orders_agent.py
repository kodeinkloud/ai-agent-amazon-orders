from swarm import Agent
import psycopg2
from config import DB_CONFIG
import os

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Get the directory containing this script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Path to database_tables.sql
schema_path = os.path.join(current_dir, "database_tables.sql")

# Read schema file
with open(schema_path, "r") as table_schema_file:
    table_schemas = table_schema_file.read()

def run_sql_select_statement(sql_statement):
    """Executes a SQL SELECT statement and returns the results of running the SELECT. Make sure you have a full SQL SELECT query created before calling this function."""
    print(f"Executing SQL statement: {sql_statement}")
    cursor.execute(sql_statement)
    records = cursor.fetchall()

    if not records:
        return "No results found."
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Calculate column widths
    col_widths = [len(name) for name in column_names]
    for row in records:
        for i, value in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(value)))
    
    # Format the results
    result_str = ""
    
    # Add header
    header = " | ".join(name.ljust(width) for name, width in zip(column_names, col_widths))
    result_str += header + "\n"
    result_str += "-" * len(header) + "\n"
    
    # Add rows
    for row in records:
        row_str = " | ".join(str(value).ljust(width) for value, width in zip(row, col_widths))
        result_str += row_str + "\n"
    
    return result_str 

def get_sql_router_agent_instructions():
    return """You are an orchestrator of different SQL data experts and it is your job to
    determine which of the agent is best suited to handle the user's request, 
    and transfer the conversation to that agent."""

def get_sql_agent_instructions():
    return f"""You are a SQL expert who takes in a request from a user for information
    they want to retrieve from the DB, creates a SELECT statement to retrieve the
    necessary information, and then invoke the function to run the query and
    get the results back to then report to the user the information they wanted to know.
    
    Here are the table schemas for the DB you can query:
    
    {table_schemas}

    Write all of your SQL SELECT statements to work 100% with these schemas and nothing else.
    You are always willing to create and execute the SQL statements to answer the user's question.
    """


sql_router_agent = Agent(
    name="Router Agent",
    instructions=get_sql_router_agent_instructions()
)
products_agent = Agent(
    name="Products Agent",
    instructions=get_sql_agent_instructions() + "\n\nHelp the user with data related to products.",
    functions=[run_sql_select_statement],
)
orders_agent = Agent(
    name="Orders Agent",
    instructions=get_sql_agent_instructions() + "\n\nHelp the user with data related to orders.",
    functions=[run_sql_select_statement],
)
order_items_agent = Agent(
    name="Order Items Agent",
    instructions=get_sql_agent_instructions() + "\n\nHelp the user with data related to order items.",
    functions=[run_sql_select_statement],
)
addresses_agent = Agent(
    name="Addresses Agent",
    instructions=get_sql_agent_instructions() + "\n\nHelp the user with data related to addresses.",
    functions=[run_sql_select_statement],
)

def transfer_back_to_router_agent():
    """Call this function if a user is asking about data that is not handled by the current agent."""
    return sql_router_agent

def transfer_to_products_agent():
    return products_agent

def transfer_to_orders_agent():
    return orders_agent

def transfer_to_order_items_agent():
    return order_items_agent

def transfer_to_addresses_agent():
    return addresses_agent

sql_router_agent.functions = [transfer_to_products_agent, transfer_to_orders_agent, transfer_to_order_items_agent, transfer_to_addresses_agent]
products_agent.functions.append(transfer_back_to_router_agent)
orders_agent.functions.append(transfer_back_to_router_agent)
order_items_agent.functions.append(transfer_back_to_router_agent)
addresses_agent.functions.append(transfer_back_to_router_agent)
