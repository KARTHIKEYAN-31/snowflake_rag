# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session
from snowflake.core import Root
import pandas as pd
import json

NUM_CHUNKS = 3 

CORTEX_SEARCH_DATABASE = "RAG_DATABASE"
CORTEX_SEARCH_SCHEMA = "RAG"
CORTEX_SEARCH_SERVICE = "RAG_SEARCH_SERVICE"
warehouse = "COMPUTE_WH"

COLUMNS = [
    "chunk",
    "relative_path",
    "file_name"
]

connection_parameters = { "account": st.secrets["ACCOUNT"], 
                         "user": st.secrets["USER"], 
                         "password": st.secrets["PASSWORD"], 
                         "role": st.secrets["ROLE"],
                         "warehouse": warehouse,
                         "database": CORTEX_SEARCH_DATABASE, 
                         "schema": CORTEX_SEARCH_SCHEMA,
                         "ssl"=True,
                         }

session = Session.builder.configs(connection_parameters).create()

# session = get_active_session()
root = Root(session)                         

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]



def set_confg():
    
    st.session_state.model_name = 'mistral-large2'
    
    query = f""" SELECT file_name FROM docs_chunks_table group by file_name"""  
    file_names_list = session.sql(query).to_pandas()['FILE_NAME'].unique().tolist()

    st.sidebar.selectbox('Select File Name', ['ALL'] +file_names_list, key = 'file_name')

def upload_file():
    uploaded_file = st.file_uploader("Choose a PDF file", type="pdf")
    if uploaded_file is not None:
        file_content = uploaded_file.read()
        stage_name = '@my_stage' 
        file_path = f"{stage_name}/{uploaded_file.name}" 
        session.file.put_stream(file_path, file_content) 
        st.toast(f"File '{uploaded_file.name}' uploaded successfully to {stage_name}")


def get_similar_chunks_search_service(query):

    if st.session_state.file_name == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else: 
        filter_obj = {"@eq": {"file_name": st.session_state.file_name} }
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    st.session_state.context = json.loads(response.json())
    st.sidebar.json(response.json())
    
    return response.json() 


def create_prompt(myquestion):
    prompt_context = get_similar_chunks_search_service(myquestion)

    prompt = f"""
       You are an expert chat assistance that extracs information from the CONTEXT provided
       between <context> and </context> tags, 
       When ansering the question contained between <question> and </question> tags
       be concise and do not hallucinate. 
       If you don´t have the information just say so.
       Only anwer the question if you can extract it from the CONTEXT provideed.
       
       Do not mention the CONTEXT used in your answer.
       
       <context>          
       {prompt_context}
       </context>
       <question>  
       {myquestion}
       </question>
       Answer: 
       """

    json_data = json.loads(prompt_context)

    relative_paths = set(item['relative_path'] for item in json_data['results'])
            
    return prompt, relative_paths

def complete(myquestion):

    prompt, relative_paths =create_prompt (myquestion)
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    return df_response, relative_paths



set_confg()
upload_file()

if 'messages' not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Hello! 👋 I'm your RAG assistant bot, here to help you find and generate the information you need. Whether it's retrieving data, answering questions, or providing insights, I've got you covered. How can I assist you today?"}]

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if question := st.chat_input("What is up?"):
    
    st.chat_message('user').markdown(question)
    st.session_state.messages.append({'role':'user', "content":question})
    
    response, relative_paths = complete(question)
    res_text = response[0].RESPONSE

    answer = f"""
    {res_text}
    
    1. {st.session_state.context['results'][0]['file_name']}
    2. {st.session_state.context['results'][1]['file_name']}
    3. {st.session_state.context['results'][2]['file_name']}"""
    
    
    st.session_state.messages.append({'role':'assistant', 'content':answer})
    st.rerun()


