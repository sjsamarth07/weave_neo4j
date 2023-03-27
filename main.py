import pandas as pd
#dict subclass with keylist/keypath support
from benedict import benedict
#driver to allow connections to db
from neo4j import GraphDatabase


def parser_data_into_quries(df: pd.DataFrame,tag: str) -> list:
    """
    iterates over the key, value pairs
    :param df: dataframe
    :return: list
    """
    transaction_list = df.values.tolist()

    transaction_execution_commands = []
    for i in transaction_list:
        row = benedict(i[0])
        query = []
        for count, each in enumerate(row.keypaths()):
            if isinstance(row[each], dict) or isinstance(row[each], list):
                continue
            query.append(f"""{each.replace('@', '').replace('.', "_").replace('#', '').replace("'","")}: '{row[each].replace("'","")}'""")
        neo4j_create_statemenet = f"create (t:{tag} " + "{" + ', '.join(query) + "})"
        transaction_execution_commands.append(neo4j_create_statemenet)

    return transaction_execution_commands

def parser_data_into_quries_1(df: pd.DataFrame) -> list:
    transaction_list = df.values.tolist()

    transaction_execution_commands = []
    for i in transaction_list:
        row = benedict(i[0])
        query = []
        for count, each in enumerate(row.keypaths()):
            if isinstance(row[each], dict) or isinstance(row[each], list):
                continue
            query.append(f"""{each.replace('@', '').replace('.', "_").replace('#', '').replace("'","")}: '{row[each].replace("'","")}'""")
        neo4j_create_statemenet = "create (t:Comment " + "{" + ', '.join(query) + "})"
        transaction_execution_commands.append(neo4j_create_statemenet)

    return transaction_execution_commands


def execute_transactions(transaction_execution_commands: list):
    data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "Sainath0794"))
    session = data_base_connection.session()
    for i in transaction_execution_commands:
        session.run(i)


if __name__ == '__main__':
    data = benedict('Q9Y261.xml', format='xml')

    tags = ['reference', 'comment','dbReference']
    for i in tags:
        df = pd.DataFrame.from_dict(data=dict(
            reference=data[f'uniprot.entry.{i}'],

        ))
        execute_transactions(parser_data_into_quries(df=df,tag=i))


