from langchain.prompts import PromptTemplate

SOURCE_PROMPT_TEMPLATE  = """Given the schema and information of the following datasources:
{source_info}
Determine which source can be used to answer the given question.

After determining the source, write the query to retrieve the information.

Return the answer as a json dictionary with 'question' containing the user question, 'source' key containing the source name and 'query' key containing the query.

For elastic search query return the query in the following format:
POST /index_name/_search
"""

SOURCE_INFO_TEMPLATE = """
Source {num}: 
name: {name}
info: {info}
<source{num}>
{source_schema}
</source{num}>
"""

class QueryPromptLoader():

    def __init__(self, datasources):
        self.datasources = datasources

    def get_parsed_source_info_str(self):
        prompt_template = PromptTemplate.from_template(SOURCE_INFO_TEMPLATE)
        parsed_source_info_str = ''
        
        for idx, key in enumerate(self.datasources):
            curr_datasource = self.datasources[key]['datasource']
            template = prompt_template.format(num=idx+1, name=key, info=curr_datasource.get_info(), source_schema=curr_datasource.get_table_schemas())
            parsed_source_info_str += template

        return parsed_source_info_str

    def get_source_prompt(self):
        source_prompt = PromptTemplate.from_template(SOURCE_PROMPT_TEMPLATE)
        source_prompt = source_prompt.format(source_info=self.get_parsed_source_info_str())
        return source_prompt