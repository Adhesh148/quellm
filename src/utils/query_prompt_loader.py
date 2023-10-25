from langchain.prompts import PromptTemplate

SOURCE_PROMPT_TEMPLATE  = """Given the schema and information of the following datasources:
{source_info}
Determine which source can be used to answer the given question.

After determining the source, write the query to retrieve the information.

Return the answer as a json dictionary with 'question' containing the user question, 'source' key containing the answer and 'query' key containing the query.
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
            template = prompt_template.format(num=idx+1, name=key, info=self.datasources[key]['info'], source_schema=self.datasources[key]['datasource'].get_table_schemas())
            parsed_source_info_str += template

        return parsed_source_info_str

    def get_source_prompt(self):
        source_prompt = PromptTemplate.from_template(SOURCE_PROMPT_TEMPLATE)
        source_prompt = source_prompt.format(source_info=self.get_parsed_source_info_str())
        # print(source_prompt)
        return source_prompt