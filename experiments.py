from plotting import plot_performance_comparison
import postgres
import mongo

if __name__ == "__main__":
    # # INSERTS
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

    # # READS
    # scaling_stages = [1_000, 2_000, 10_000, 100_000]
    # read_timings: list[float] = [test_read_performance(init_func_n=n) for n in scaling_stages]  # type: ignore
    # plot_performance_test(
    #     title='Postgres Read Performance (Songs in Playlist View)',
    #     time_taken=read_timings,
    #     scaling_stages=scaling_stages,
    # )
