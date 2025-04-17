--> Working with Retrieval Augmented Generation with Snowflake LLM Cortex Search and Cortex LLM Functions

--> Create a database
create database cortex_search_docs_basic;
--> Create a schema under the database
create schema data;

select * from docs_chunks_table;

/*
Ingestion of all the document inside the Snowflake
1. Used UDFTs to create a table with chunks of the pdf docs through langchain
*/

--> Create user defined table function that will split the text into chunks and return the values as table
create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as 
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:
    def process(self, pdf_text:str):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512,
            chunk_overlap = 256,
            length_function = len
        )

        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])

        yield from df.itertuples(index=False, name=None)
$$
;

--> Create a stage with directory table where you will be uploading your documents
create or replace stage docs encryption = (type = 'snowflake_sse') directory = (enable = true);

--> List all the docs
ls @docs;

/*
Data Transformation Steps
1. Create a Table where we are going to store the chunks for each pdf
2. Use Snowflake Cortex LLM Function snowflake.cortex.parse_document to read the pdf documents directly from snowflake stage.
3. Label the product category - Adding metadaata filter using snowflake cortex llm function called complete - with the prompt "Given the name of the file, select what category it is"
*/

create or replace table docs_chunks_table (
relative_path varchar(16777216), -- Relative path to the pdf file
size number(38,0), -- Size of the pdf
file_url varchar(16777216), -- URL for the pdf
scoped_file_url varchar(16777216), -- Scoped URL (you can choose which one depending on your use case) Snowflake Hosted URL
chunk varchar(16777216), -- Piece of text
category varchar(16777216) -- Will hold the document category (will be enabled next)
);

insert into docs_chunks_table(relative_path, size, file_url, scoped_file_url, chunk)
select
    relative_path, 
    size, 
    file_url, 
    build_scoped_file_url(@docs, relative_path) as scoped_file_url, 
    func.chunk as chunk 
from directory(@docs),
    table(text_chunker(to_varchar(snowflake.cortex.parse_document(@docs, relative_path, {'mode':'layout'})))) as func;

--> For Step 3: Create a Temporary Table as we are not going to use the table after this step
create or replace temporary table docs_categories as with 
unique_documents as (
select distinct relative_path from docs_chunks_table
),
docs_category_cte as (
select 
    relative_path,
    trim(snowflake.cortex.complete(
    'llama3-70b',
    'Given the name of the file between <file> and </file> determine if it is related to bikes or snow. Use only one word <file>' || relative_path || '</file>'
    ), '\n') as category
    from unique_documents
)
select * from docs_category_cte;
--> with clause for Common Table Expression so that one CTE is used within other and manipulations are done. 

select * from docs_categories;

--> Finally let's update the docs_chunks_table
update docs_chunks_table set category = docs_categories.category from docs_categories where docs_chunks_table.relative_path = docs_categories.relative_path;

select * from docs_chunks_table;

/*
Cortex Search Service
1. The Service uses Chunks columns to create embeddings and perform retrieval based on similarity search
2. column category is used as an filter
3. service will be refreshed every 1 minute
4. data retrieved from table contains chunk, relative path, file url, and category
*/

create or replace cortex search service search_service_data
on chunk
attributes category
warehouse = COMPUTE_WH
target_lag = '1 minute'
as (
select 
    chunk, 
    relative_path,
    file_url,
    category
from docs_chunks_table
);


--> For automatic processing of new documents
create or replace stream docs_stream on stage docs;

--> Task has been created for the stream
create or replace task parse_and_insert_pdf_task
warehouse = COMPUTE_WH
schedule = '1 minute'
when system$stream_has_data('docs_stream')
as

insert into docs_chunks_table (relative_path, size, file_url, scoped_file_url, chunk)
select
    relative_path, 
    size, 
    file_url, 
    build_scoped_file_url(@docs, relative_path) as scoped_file_url, 
    func.chunk as chunk
from directory(@docs),
    table(text_chunker(to_varchar(snowflake.cortex.parse_document(@docs, relative_path, {'mode':'layout'})))) as func;

alter task parse_and_insert_pdf_task resume; --> Resume the task
--> Upload the pdf in the docs stage

select * from docs_stream;

--> Suspend the task
alter task parse_and_insert_pdf_task suspend; --> Once all the work has been completed, you can suspend the task


--> Let's cleanup 
drop database cortex_search_docs_basic;
