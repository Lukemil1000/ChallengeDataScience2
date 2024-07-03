from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def create_session():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Semana2") \
        .getOrCreate()
    return spark

def main():
    spark = create_session()

    dataframe = spark.read.json('D:\Projetos\ChallengeDataScience2\ChallengeDataScience2\Semana1\dataset\dataset_bruto.json')
    df_anuncio = dataframe.select('anuncio.*')
    df_anuncio_filter = df_anuncio.where((f.col('tipo_uso') == 'Residencial') & (f.col('tipo_unidade') == 'Apartamento') & (f.col('tipo_anuncio') == 'Usado'))
    df_anuncio_filter = df_anuncio_filter.withColumn('area_total', f.explode(df_anuncio_filter.area_total)) \
        .withColumn('area_util', f.explode(df_anuncio_filter.area_util)) \
        .withColumn('vaga', f.explode(df_anuncio_filter.vaga)) \
        .withColumn('banheiros', f.explode(df_anuncio_filter.banheiros)) \
        .withColumn('suites', f.explode(df_anuncio_filter.suites)) \
        .withColumn('quartos', f.explode(df_anuncio_filter.quartos))
    df_anuncio_filter = df_anuncio_filter.withColumns({'bairro': df_anuncio_filter.endereco.bairro, 'zona': df_anuncio_filter.endereco.zona}).drop('endereco')
    df_valores = df_anuncio_filter.select('id', f.explode('valores').alias('valores')).select('id', 'valores.*')
    df_anuncio_filter = df_anuncio_filter.drop('valores').join(df_valores, 'id', how='inner')
    df_anuncio_filter = df_anuncio_filter.where(f.col('tipo') == 'Venda')
    df_anuncio_filter.write.parquet(path='D:\Projetos\ChallengeDataScience2\ChallengeDataScience2\Semana1\dataset\dataset_final_parquet', mode='overwrite')

    spark.stop()
    print('Acabou')

if __name__ == "__main__":
    main()