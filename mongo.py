import time
from functools import wraps
from typing import Any, Callable, Optional

import pymongo
from faker import Faker
from pymongo.database import Database
from testcontainers.mongodb import MongoDbContainer

faker: Faker = Faker()


def mongo_performance_test(init_func: Optional[Callable] = None) -> Callable:
    def decorator(test_func: Callable) -> Callable:
        @wraps(test_func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with MongoDbContainer() as mongo:
                connection_string: str = mongo.get_connection_url()
                mongo_client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
                mongo_db: Database = mongo_client["DBIMusicPlayer"]

                if init_func:
                    init_func(mongo_db)

                start_time: float = time.perf_counter()
                result: Any = test_func(mongo_db, *args, **kwargs)
                end_time: float = time.perf_counter()

                time_taken: float = end_time - start_time
                print(f"'{test_func.__name__}' Completed - Time taken: {time_taken:.4f} seconds")

                return result

        return wrapper

    return decorator


def init_mongo_db(mongo_db: Database) -> None:
    collection = mongo_db.test_collection
    # Seed the database with data
    collection.insert_many([{"number": i} for i in range(1000)])


@mongo_performance_test()
def test_data_insertion_performance(mongo_db: Database) -> None:
    collection = mongo_db.test_collection

    for i in range(1000):
        collection.insert_one({"number": i})


@mongo_performance_test(init_func=init_mongo_db)
def test_read_performance(mongo_db: Database) -> None:
    collection = mongo_db.test_collection
    # Performance testing for read operations
    _ = collection.find({})


if __name__ == "__main__":
    test_data_insertion_performance()
    test_read_performance()
