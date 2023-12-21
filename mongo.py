import time
from functools import wraps
from typing import Any, Callable

import pymongo
from faker import Faker
from pymongo.database import Database
from testcontainers.mongodb import MongoDbContainer

faker: Faker = Faker()


def mongo_performance_test(test_func: Callable) -> Callable:
    @wraps(test_func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with MongoDbContainer() as mongo:
            connection_string: str = mongo.get_connection_url()
            mongo_client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
            mongo_db: Database = mongo_client["DBIMusicPlayer"]

            start_time: float = time.perf_counter()
            result: Any = test_func(mongo_db, *args, **kwargs)
            end_time: float = time.perf_counter()

            time_taken: float = end_time - start_time
            print(f"'{test_func.__name__}' Completed - Time taken: {time_taken:.4f} seconds")

            return result

    return wrapper


@mongo_performance_test
def test_data_insertion_performance(mongo_db: Database) -> None:
    collection = mongo_db.test_collection

    for i in range(1000):
        collection.insert_one({"number": i})


if __name__ == "__main__":
    test_data_insertion_performance()
