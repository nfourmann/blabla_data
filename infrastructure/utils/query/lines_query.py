from jinja2 import Environment
from contracts.ovapi_contract import line_destination

###############################################
##  VARIABLE
###############################################
full_table_name = ".".join(
    [
        line_destination.get("db_name"),
        line_destination.get("schema_name"),
        line_destination.get("table_name"),
    ]
)

columns = line_destination.get("output_schema")

env = Environment()

###############################################
##  QUERY TEMPLATE
###############################################

query_table_creation_template = """
    CREATE TABLE IF NOT EXISTS {full_table_name} (
    {% set columns_count = columns | length %}
    {% for column in columns %}
        {% if loop.counter == columns_count %}
            {{ column }} VARCHAR
        {% else %}
            {{ column }} VARCHAR,
        {% endif %}
        
    {% endfor %}

   
    ); """

query_insertion_template = """
    INSERT INTO {{full_table_name}} ({{list_columns}}) 
    VALUES(
    
        {% set records_count = new_records | length %}

        {% for record in new_records %}
            {% if loop.counter == records_count %}
                {{ record }}
            {% else %}
                {{ record }}
            {% endif %}
                
        {% endfor %}     
    )
"""

###############################################
##  QUERY RENDER
###############################################
query_table_creation = env.from_string(query_table_creation_template).render(
    full_table_name=full_table_name, columns=columns
)


query_insertion = env.from_string(query_insertion_template).render(
    full_table_name=full_table_name,
    columns=("col_test_1", "col_tet_2"),
    new_records=[("test", "test"), ("test", "test")],
)
