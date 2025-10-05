# ****************************************************************************
# ****************************************************************************
# CODE BLOCK 1: START STREAMLIT, INITIAL SETUP, UI CONFIG
# ****************************************************************************
# ****************************************************************************

# -----------------------------------------------
# START STREAMLIT IN SNOWFLAKE & IMPORT LIBRARIES
# ----------------------------------------------
import streamlit as st
import pandas as pd
import streamlit as stss
from snowflake.snowpark import Session
from snowflake.connector.errors import DatabaseError
import time

# Configure page layout settings, set app title and description
st.set_page_config(layout="wide")
escaped_question = ""

# Snowflake connection parameters
connection_parameters = {
    "account": "a3209653506471-sales-eng-demo",  #123s e.g., "xy12345.us-east-1"
    "user": "doug.jauregui@fivetran.com",
    "password": "Jaur0131!@",  #s Consider using st.secrets for sensitive data
    "role": "SALES_ENGINEERING",  # e.g., "ACCOUNTADMIN"
    "warehouse": "DEFAULT",
    "database": "DOUG_JAUREGUI",
    "schema": "HEIGHT"
}

# Create Snowflake session
def create_session(max_retries=4):
    for attempt in range(max_retries):
        try:
            session = Session.builder.configs(connection_parameters).create()
            return session
        except DatabaseError as e:
            if attempt == max_retries - 1:
                st.error(f"Failed to connect after {max_retries} attempts: {str(e)}")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff

# Initialize session
session = create_session()

# -----------------------------------------------
# INITIAL SETUP AND SESSION MANAGEMENT
# -----------------------------------------------

# Initialize internal state variables to manage application state across sessions
if 'internal_context' not in st.session_state:
    st.session_state['internal_context'] = False  # Assuming False is the default state

# Function to update dataset context usage based on user interaction with the sidebar checkbox
def update_context_usage(checkbox_value):
    st.session_state['use_dataset_as_context'] = checkbox_value

# Check and initialize session state variable for dataset context usage
if 'use_dataset_as_context' not in st.session_state:
    st.session_state['use_dataset_as_context'] = False  # Default can be True or False based on typical use

# Pre-define the model name based on user selection from the sidebar
# This variable will be used throughout the session to direct queries to Snowflake Cortex
model = None

# -----------------------------------------------
# UI CONFIGURATION
# -----------------------------------------------
def sanitize_sql_string(s):
    return s.replace("'", "''")

# Configure page layout settings, set app title and description
# st.set_page_config(layout="wide")
st.title(":rocket: Superior Support Engineer Assistant :rocket:")
st.caption (" 0.9.16.24")

# Introductory text explaining the app's purpose, mentioning integration with major tech partners
# This text is intended to provide users with context about the data and tools powering the app
st.write(
    """I'm an interactive Height and Zendesk ticket provider. A bit about me...I'm a RAG-based, Gen AI app **built with and powered by FIVETRAN!"""
)

st.caption(
    """Let me help with your support needs in the Snowflake Data Cloud with Fivetran, I'll assist you with HEIGHT information and provide visit recommendations from numerous models available in Snowflake Cortex (including Snowflake Arctic). You can even pick the model you want to use or try out all the models. The dataset includes ALl of HEIGHT APP indexed along with possible ZENDESK tickets. Let's get started!"""
)

# ****************************************************************************
# ****************************************************************************
# CODE BLOCK 2: USER INPUT MGMT, PROCESSING AND RESPONSE HANDLING
# ****************************************************************************
# ****************************************************************************

# -----------------------------------------------
# USER INPUT MANAGEMENT
# -----------------------------------------------

# Initialize or retrieve the current user question from session state
if 'user_question' not in st.session_state:
    st.session_state['user_question'] = ""

user_question_placeholder = "Message your personal Support Engineering Assistant..."

# Define a dynamic key for the text input box to force a re-render when the 'Reset conversation' button is pressed
# This approach prevents stale data from persisting in the input field after the session state is reset
if 'reset_key' not in st.session_state:
    st.session_state['reset_key'] = 0

# Implement button logic for resetting the conversation, preserving checkbox state to maintain user settings across reruns
# This prevents unexpected behavior and improves user experience by keeping their preferences intact after a reset
if st.button('Reset conversation', key='reset_conversation_button'):
    st.session_state['conversation_state'] = []
    st.session_state['user_question'] = ""  # Clear the question from the input field and the session state
    checkbox_state = st.session_state['use_dataset_as_context']  # Preserve checkbox state before rerun
    st.session_state['reset_key'] += 1  # Increment to change the key of the text area, forcing a re-render
    st.session_state['use_dataset_as_context'] = checkbox_state  # Restore checkbox state after rerun
    st.experimental_rerun()

# Placeholder for processing message
processing_placeholder = st.empty()

# Define the text input box with a dynamic key, modified for simple 'Enter' submission
question = st.text_input("", value=st.session_state['user_question'], placeholder=user_question_placeholder, key=f"text_input_{st.session_state['reset_key']}", label_visibility="collapsed")

# -----------------------------------------------
# PROCESSING AND RESPONSE HANDLING
# -----------------------------------------------

# Display a temporary processing message while handling user queries, which enhances user experience by providing feedback on system status
if question:
    if 'user_question' not in st.session_state or st.session_state['user_question'] != question:
        st.session_state['user_question'] = question  # Update the session state with the new question

        # Display a processing message while the app handles the question
        processing_placeholder.caption(f"I'm thinking about your question: {question}")
        
        # Clear the processing message
    processing_placeholder.empty()

        # Optionally clear the question from the input after processing
    st.session_state['user_question'] = ""

# ****************************************************************************
# CODE BLOCK 3: MODEL SELECTION AND DATA CONTEXT
# ****************************************************************************
# ****************************************************************************

# -----------------------------------------------
# MODEL SELECTION AND DATA CONTEXT
# -----------------------------------------------

# Sidebar for model selection provides options to choose from different Snowflake Cortex models
# Each model has different capabilities and performance characteristics, affecting how user queries are processed
model = st.sidebar.selectbox('Select a Snowflake Cortex model:', (
    'llama3.2-3b',
    'openai-gpt-oss-120b', 
    'llama3.3-70b',
    'claude-sonnet-4-5',
    'open-gpt-5',
    'llama3.2-8b'
))

# Updates the 'use_dataset_as_context' in session state based on user interaction with the checkbox
# This function toggles the context setting, affecting how the application processes user questions
def update_context_usage(checkbox_value):
    st.session_state['use_dataset_as_context'] = checkbox_value

# Checkbox for user to decide if their Fivetran dataset should be used as context
st.sidebar.checkbox(
    'Use your Fivetran dataset as context?', 
    value=st.session_state['use_dataset_as_context'], 
    key="dataset_context",
    on_change=lambda: st.session_state.update({'use_dataset_as_context': not st.session_state['use_dataset_as_context']})
)

# Sidebar description for Snowflake Cortex
st.sidebar.caption(
    """I use **Snowflake Cortex** which provides instant access to industry-leading large language models (LLMs), including **Snowflake Arctic**, trained by researchers at companies like Mistral, Meta, Google, Reka, and Snowflake.\n"""
    """\n"""
    """Cortex also offers models that Snowflake has fine-tuned for specific use cases. Since these LLMs are fully hosted and managed by Snowflake, using them requires no setup. My data stays within Snowflake, giving me the performance, scalability, and governance you expect."""
)
# -----------------------------------------------
# SIDEBAR CONFIGURATIONS FOR VECTOR SEARCH
# -----------------------------------------------

# Add vector search configurations to sidebar
st.sidebar.markdown("### Vector Search Settings")

# Distance threshold slider (lower means more similar)
search_threshold = st.sidebar.slider(
    'Similarity Threshold',
    min_value=0.0,
    max_value=2.0,
    value=0.8,
    step=0.1,
    help='Lower values mean more similar results. Adjust this to control how closely matches should relate to your query.'
)

# Number of related tickets to consider
num_tickets = st.sidebar.number_input(
    'Number of Related Tickets',
    min_value=1,
    max_value=7,
    value=3,
    help='Number of similar tickets to consider when providing an answer.'
)

# ****************************************************************************
# ****************************************************************************
# CODE BLOCK 4: ADDITIONAL UI ELEMENTS, CONVERSATION HISTORY INITIALIZATION
# ****************************************************************************
# ****************************************************************************

# -----------------------------------------------
# ADDITIONAL UI ELEMENTS
# -----------------------------------------------

# Display tech stack logos at the bottom of the sidebar to visually endorse the technologies used and to strengthen branding
for _ in range(12):  # Adjust the range to add more or less space as needed
    st.sidebar.write("")  # This adds empty lines to the sidebar

# # Display tech stack logos and captions - the image is hosted in a publicly accessible Imgur repo
url = 'https://i.imgur.com/9lS8Y34.png'

# Use columns to center the image
col1, col2, col3 = st.sidebar.columns([1,2,1.3])  # Adjust the ratio to better fit your sidebar width
with col2:  # Middle column for the image
    st.image(url, width=150)  # Display the image with a specific width

# Separate columns for the caption to visually center it
caption_col1, caption_col2, caption_col3 = st.sidebar.columns([0.22,2,0.005])
with caption_col2:
    st.caption("Fivetran, Snowflake, Streamlit & Cortex")  # Display the caption using st.caption

# -----------------------------------------------
# CONVERSATION HISTORY INITIALIZATION
# -----------------------------------------------

# Initialize conversation history using session state
if 'conversation_state' not in st.session_state:
    st.session_state['conversation_state'] = []

conversation_state = st.session_state['conversation_state']  # Access from session state


# ****************************************************************************
# ****************************************************************************
# CODE BLOCK 5: SNOWFLAKE INTERACTION LOGIC W/ CORTEX AND CONVO HISTORY MGMT
# ****************************************************************************
# ****************************************************************************

# -----------------------------------------------
# SNOWFLAKE INTERACTION LOGIC WITH CORTEX
# -----------------------------------------------

# Implement Snowflake interaction logic based on the session state
if st.session_state['use_dataset_as_context']:
    # SQL and caption logic for when the checkbox is checked
    st.caption("Please note that :green[**_I am_**] using your Fivetran dataset as context. All models are very creative and can make mistakes. Consider checking important information before looking in height for your manual boring search.")
    question = sanitize_sql_string(question)
    sql = f"""
  select snowflake.cortex.complete(
        '{model}', 
        concat(
            'Act as a software support engineer expert for sales engineers who want expert information on issues with their connector. You are a expert support assistant named Darth Vader Support Assistant. Provide the most accurate information on Connector details with Height tickets and Zendesk numbers based on HEIGHT from DOUG_JAUREGUI.',
            'Context: ', 
            (
                WITH RANKED_MATCHES AS (
                    SELECT 
                        VECTOR_TEXT,
                        vector_l2_distance(
                            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('e5-large-v2', '{question}'), 
                            HEIGHT_EMBEDDING
                        ) as distance,
                        ROW_NUMBER() OVER (ORDER BY vector_l2_distance(
                            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('e5-large-v2', '{question}'), 
                            HEIGHT_EMBEDDING
                        )) as rank
                    FROM DOUG_JAUREGUI.HEIGHT.single_string_height_review_vector
                    WHERE vector_l2_distance(
                        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('e5-large-v2', '{question}'), 
                        HEIGHT_EMBEDDING
                    ) < {search_threshold}
                )
                SELECT LISTAGG(VECTOR_TEXT, ' --- Next Relevant Ticket --- ') 
                FROM RANKED_MATCHES 
                WHERE rank <= {num_tickets}
            ), 
            'Question: ', 
            '{question}', 
            'Instructions: ',
            '1. Focus on the most relevant ticket information first.',
            '2. If multiple tickets are provided, synthesize the information coherently.',
            '3. Include specific ticket IDs and Zendesk references.',
            '4. Include specific pull request from github and the API added into what version of HVR.',
            '5. If no close matches are found, clearly state that.',
            '6. Maintain technical accuracy in all responses.',
            'Response: '
        )
    ) as response;
    """

else:
    # SQL and caption logic for when the checkbox is unchecked
    st.caption("Please note that :red[**_I am NOT_**] using your Fivetran dataset as context. All models are very creative and can make mistakes. Consider checking important information before responding to customers or reaching out to fellow SEs.")
    sql = f"""
    select snowflake.cortex.complete('{model}', '{question}') as response;
    """

# Handle SQL execution and response retrieval errors to ensure the application remains robust and user-friendly
# Potential actions on errors include logging for debugging, user notifications, or retries for transient issues    
if question:
    try:
        data = session.sql(sql).collect()
        response = data[0][0]  # Assuming a single response

        # Append question and response to conversation history (in reverse order)
        # Append the question and response with the model name to conversation history
        conversation_state.append((f"Expert Support Assistant ({model}):", response))
        conversation_state.append(("You:", question))
        #conversation_state.append(response)
        #conversation_state.append(question)

    except Exception as e:
        st.warning(f"An error occurred while processing your question: {e}")

# -----------------------------------------------
# CONVERSATION HISTORY MANAGEMENT
# -----------------------------------------------

# Display conversation history in reverse order
if conversation_state:
    for i in reversed(range(len(conversation_state))):  # Iterate in reverse order
        label, message = conversation_state[i]
        if i % 2 == 0:  # Even index: Assistant response
            st.write(f":rocket:**{label}** {message}")
        else:  # Odd index: User question
            st.write(f":question:**{label}** {message}")
    
# Display conversation history in reverse order
#if conversation_state:
    #for i in reversed(range(len(conversation_state))):  # Iterate in reverse order
        #message = conversation_state[i]
        #if i % 2 == 0:  # Even index: User question
            #st.write(f":rocket:**CA Wine Country Visit Assistant ({model}):** {message}") # Question icon (adjust based on your icon library) and include model name
        #else:  # Odd index: Assistant response
            #st.write(f":question:**You:** {message}")  # Robot icon (adjust based on your icon library)

# Clear the prompt after displaying the response
st.empty()  # This clears the text input 




