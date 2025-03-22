from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("genshin")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_genshin="dataset.csv"
    df_genshin = spark.read.csv(path_genshin,header=True,inferSchema=True)
    df_genshin = df_genshin.withColumnRenamed("character_name", "name")
    df_genshin = df_genshin.withColumnRenamed("birthday", "birth")
    df_genshin = df_genshin.withColumnRenamed("star_rarity", "rarity")
    df_genshin = df_genshin.withColumnRenamed("weapon_type", "weapon")
    df_genshin.createOrReplaceTempView("genshin")
    query='DESCRIBE genshin'
    spark.sql(query).show(20)

    # Personajes de Mondstadt
    query="""SELECT name, birth FROM genshin WHERE region=="Mondstadt" ORDER BY `birth`"""
    df_genshin_names = spark.sql(query)
    df_genshin_names.show(20)

    # Selección de personajes de tipo espada
    query = """SELECT name FROM genshin WHERE weapon = 'Sword'"""
    df_result = spark.sql(query)
    df_result.show()
    
    # Contar personajes por rareza
    query = """SELECT rarity, COUNT(*) AS count FROM genshin GROUP BY rarity ORDER BY count DESC"""
    df_result = spark.sql(query)
    df_result.show()

    # Personajes de Sumeru con rareza de 5 estrellas
    query = """SELECT name FROM genshin WHERE region = 'Sumeru' AND rarity = 5"""
    df_result = spark.sql(query)
    df_result.show()

    # Ordenar personajes por fecha de lanzamiento más reciente
    query = """SELECT name, release_date FROM genshin ORDER BY release_date DESC"""
    df_result = spark.sql(query)
    df_result.show()
    
    results = df_genshin_names.toJSON().collect()
    parsed_results = [json.loads(r) for r in results]

    with open('results/data.json', 'w') as file:
        json.dump(parsed_results, file, indent=2)

    query = """SELECT vision, COUNT(*) AS count FROM genshin GROUP BY vision ORDER BY count DESC"""
    df_result = spark.sql(query)
    df_result.show()
    
    # Contar personajes por visión (tipo de elemento)
    query = """SELECT vision, COUNT(*) AS count FROM genshin GROUP BY vision ORDER BY count DESC"""
    df_result = spark.sql(query)
    df_result.show()

    # Personajes que usan arco y son rareza de 4 estrellas
    query = """SELECT name FROM genshin WHERE weapon = 'Bow' AND rarity = 4"""
    df_result = spark.sql(query)
    df_result.show()

    # Cantidad de personajes por región, ordenados de mayor a menor
    query = """SELECT region, COUNT(*) AS count FROM genshin GROUP BY region ORDER BY count DESC"""
    df_result = spark.sql(query)
    df_result.show()

    spark.stop()