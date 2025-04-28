from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys


def get_products_with_categories(products_df, categories_df, product_category_df):
    products = products_df.alias('products')
    categories = categories_df.alias('categories')
    prod_cat = product_category_df.alias('prod_cat')

    result_df = products.join(
        prod_cat,
        on='product_id',
        how='left'
    ).join(
        categories,
        on=F.col('prod_cat.category_id') == F.col('categories.category_id'),
        how='left'
    ).select(
        F.col('products.product_name'),
        F.col('categories.category_name')
    )

    return result_df


def main():
    # Настройки окружения для стабильной работы PySpark
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'

    spark = SparkSession.builder \
        .appName('ProductsCategoriesExample') \
        .master('local[1]') \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.python.worker.timeout", "120") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    # Тестовые данные
    products_data = [
        (1, 'Apple'),
        (2, 'Banana'),
        (3, 'Carrot'),
        (4, 'Donut'),
    ]
    categories_data = [
        (10, 'Fruits'),
        (20, 'Vegetables'),
        (30, 'Sweets'),
    ]
    product_category_data = [
        (1, 10),
        (2, 10),
        (3, 20),
    ]

    # Создание DataFrame
    products_df = spark.createDataFrame(
        products_data, ['product_id', 'product_name'])
    categories_df = spark.createDataFrame(
        categories_data, ['category_id', 'category_name'])
    product_category_df = spark.createDataFrame(
        product_category_data, ['product_id', 'category_id'])

    # Получение результатов
    result_df = get_products_with_categories(
        products_df, categories_df, product_category_df)

    # Вывод результатов
    print('Все продукты с категориями (если есть):')
    result_df.show()

    print('\nПродукты без категорий:')
    result_df.filter(F.col('category_name').isNull()
                     ).select('product_name').show()

    spark.stop()


if __name__ == '__main__':
    main()
