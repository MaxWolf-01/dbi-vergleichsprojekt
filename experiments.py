from plotting import plot_performance_comparison
import postgres
import mongo


def test_inserts() -> None:
    scaling_stages = [250, 500, 1_000, 2_000]
    insert_one_timings_mongo: list[float] = [mongo.test_insert_performance(n=n) for n in scaling_stages]  # type: ignore
    insert_one_timings_pg: list[float] = [postgres.test_insert_performance(n=n) for n in scaling_stages]  # type: ignore
    insert_many_timings_pg: list[float] = [  # type: ignore
        postgres.test_insert_many_performance(n=n) for n in scaling_stages  #
    ]
    insert_many_timings_mongo: list[float] = [  # type: ignore
        mongo.test_insert_many_performance(n=n) for n in scaling_stages
    ]
    plot_performance_comparison(
        title='MongoDB vs Postgres - Insert vs Insert Many Performance Comparison',
        timings=[
            insert_one_timings_mongo,
            insert_many_timings_mongo,
            insert_one_timings_pg,
            insert_many_timings_pg,
        ],
        scaling_stages=scaling_stages,
        labels=['MongoDB Insert', 'MongoDB Insert Many', 'Postgres Insert', 'Postgres Insert Many'],
    )


def test_reads() -> None:
    scaling_stages = [250, 500, 1_000, 2_000]
    read_timings_mongo: list[float] = [mongo.test_read_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    read_timings_pg: list[float] = [postgres.test_read_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    plot_performance_comparison(
        title='MongoDB vs Postgres - Read Performance Comparison',
        timings=[read_timings_mongo, read_timings_pg],
        scaling_stages=scaling_stages,
        labels=['MongoDB Read', 'Postgres Read'],
    )


def test_deletes() -> None:
    scaling_stages = [250, 500, 1_000, 2_000]
    delete_timings_mongo: list[float] = [mongo.test_delete_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    delete_timings_pg: list[float] = [postgres.test_delete_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    plot_performance_comparison(
        title='MongoDB vs Postgres - Delete Performance Comparison',
        timings=[delete_timings_mongo, delete_timings_pg],
        scaling_stages=scaling_stages,
        labels=['MongoDB Delete', 'Postgres Delete'],
    )


def test_updates() -> None:
    scaling_stages = [250, 500, 1_000, 2_000]
    update_timings_mongo: list[float] = [mongo.test_update_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    update_timings_pg: list[float] = [postgres.test_update_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    plot_performance_comparison(
        title='MongoDB vs Postgres - Update Performance Comparison',
        timings=[update_timings_mongo, update_timings_pg],
        scaling_stages=scaling_stages,
        labels=['MongoDB Update', 'Postgres Update'],
    )


if __name__ == "__main__":
    # test_inserts()
    # test_reads()
    # test_deletes()
    test_updates()
