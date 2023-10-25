from langchain.prompts import ChatPromptTemplate
from langchain.sql_database import SQLDatabase
from langchain.chat_models import ChatOpenAI

import chainlit as cl
from tabulate import tabulate
import dataframe_image as dfi
import pandas as pd

import json 
import os

open_api_key = os.environ["OPENAI_API_KEY"]

initial_prompt = """ Given the schema and information of the following datasources:

Source 1: a SQL database about portfolio and asset returns
<source1>
{source1}
</source1>

Source 2: a SQL database about store tables
<source2>
{source2}
</source2>

Determine which source can be used to answer the given question. The answer must be one of the following: "source1", "source2" or "none".

After determining the source, write the query to retrieve the information.

Return the answer as a json dictionary with 'question' containing the user question, 'source' key containing the answer and 'query' key containing the query.
"""

def get_datasources():
    # define datasource #1
    source1_db = SQLDatabase.from_uri("mysql+pymysql://root:123_Phoenix@127.0.0.1/dbllm",sample_rows_in_table_info=0)
    source1_tables_info_str = source1_db.get_table_info()

    # define datasource #2
    source2_db = SQLDatabase.from_uri("mysql+pymysql://root:123_Phoenix@127.0.0.1/storedb",sample_rows_in_table_info=0)
    source2_tables_info_str = source2_db.get_table_info()

    return {
        "source1": {
            "table_info": source1_tables_info_str,
            "db_conn": source1_db
        },
        "source2": {
           "table_info": source2_tables_info_str,
            "db_conn": source2_db
        }
    }

def to_dict(response):
    return json.loads(response.content)


def get_chain(): 
    prompt = ChatPromptTemplate.from_messages([("system", initial_prompt), ("human", "{question}")])

    chain = {
        "source1": lambda x: x["source1"],
        "source2": lambda x: x["source2"],
        "question": lambda x: x["question"],
    } | prompt | ChatOpenAI() | to_dict

    return chain

@cl.on_chat_start
async def main():
    # Instantiate the chain for that user session
    chain = get_chain()
    data_sources = get_datasources()

    # Store the chain in the user session
    cl.user_session.set("chain", chain)
    cl.user_session.set("datasources", data_sources)

async def build_query(chain, data_sources, message):
    response = chain.invoke({"question": message.content, "source1": data_sources["source1"]["table_info"], "source2": data_sources["source2"]["table_info"]})
    
    msg = cl.Message(
        content=response["query"],
        author="query",
        language="sql",
        parent_id=message.id
    )
    await msg.send()

    return response

async def run_query(parent_id, response, data_sources):
    source = response["source"]
    query = response["query"]
    src_db = data_sources[source]["db_conn"]

    query_response = src_db._execute(query)

    # Convert JSON data to a list of dictionaries
    # data_list = [list(item.values()) for item in query_response]
    # headers = list(query_response[0].keys())
    # pretty_table_data = tabulate(data_list, headers, tablefmt="pretty")
    # msg = cl.Message(
    #     content=pretty_table_data,
    #     author="output",
    #     parent_id=parent_id
    # )

    df = pd.DataFrame.from_dict(query_response)
    dfi.export(df, 'query_output.png')

    # Sending an image with the local file path
    elements = [
        cl.Image(name="query_output", display="inline", path="./query_output.png", size="large")
    ]

    await cl.Message(content="Query Output", elements=elements).send()    
    
    
@cl.on_message
async def on_message(message: cl.Message):
    # print("Received: ", message.content)

    # get chain and datasources
    chain = cl.user_session.get("chain")
    data_sources = cl.user_session.get("datasources")
    
    # get the query
    query_response = await build_query(chain, data_sources, message)
    
    # run the query
    query_output = await run_query(message.id, query_response, data_sources)