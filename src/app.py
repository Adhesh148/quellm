from langchain.prompts import ChatPromptTemplate
from langchain.chat_models import ChatOpenAI

from utils.datasource_loader import DataSourceLoader
from utils.query_prompt_loader import QueryPromptLoader

import chainlit as cl
import dataframe_image as dfi
import pandas as pd

import json 
import os

open_api_key = os.environ["OPENAI_API_KEY"]

def to_dict(response):
    return json.loads(response.content)


def get_chain(source_prompt): 
    prompt = ChatPromptTemplate.from_messages([("system", source_prompt), ("human", "{question}")])

    chain = {
        "question": lambda x: x["question"],
    } | prompt | ChatOpenAI() | to_dict

    return chain

@cl.on_chat_start
async def main():
    
    # Load datasources
    datasource_loader = DataSourceLoader()
    data_sources = datasource_loader.load_datasources()

    # Load prompts
    prompt_loader = QueryPromptLoader(data_sources)
    source_prompt = prompt_loader.get_source_prompt()

    # Instantiate the chain for that user session
    chain = get_chain(source_prompt)

    # Store the chain in the user session
    cl.user_session.set("chain", chain)
    cl.user_session.set("datasources", data_sources)

async def build_query(chain, message):
    response = chain.invoke({"question": message.content})
    print(response)
    
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
    src_db = data_sources[source]["datasource"]

    query_response = src_db.run_query(query)

    df = pd.DataFrame.from_dict(query_response)
    dfi.export(df, 'query_output.png')

    # Sending an image with the local file path
    elements = [
        cl.Image(name="query_output", display="inline", path="./query_output.png", size="large")
    ]

    await cl.Message(content="Query Output", elements=elements, parent_id=parent_id).send()    
    
    
@cl.on_message
async def on_message(message: cl.Message):
    # get chain and datasources
    chain = cl.user_session.get("chain")
    data_sources = cl.user_session.get("datasources")
    
    # get the query
    query_response = await build_query(chain, message)
    
    # run the query
    query_output = await run_query(message.id, query_response, data_sources)