from plotting import plot_performance_comparison
import postgres
import mongo

SCALING_STAGES = [100, 1_000, 100_000]


def extrapolate_performance(
        mean_std: tuple[float, float], original_n: int, scaling_stages=None
) -> list[tuple[float, float]]:
    scaling_stages = scaling_stages or SCALING_STAGES

    def scale_performance(scale: int) -> tuple[float, float]:
        mean, std = mean_std
        factor = scale / original_n
        return mean * factor, std * factor

    return [scale_performance(scale) for scale in scaling_stages]


def test_inserts() -> None:
    n = 100
    n_tests = 1000
    insert_one_mean_std_mongo: tuple[float, float] = mongo.test_insert_performance(n=n, n_tests=n_tests)  # type: ignore
    insert_one_mean_std_pg: tuple[float, float] = postgres.test_insert_performance(n=n, n_tests=n_tests)  # type: ignore
    insert_many_mean_std_pg: tuple[float, float] = postgres.test_insert_many_performance(n=n, n_tests=n_tests)  # type: ignore
    insert_many_mean_std_mongo: tuple[float, float] = mongo.test_insert_many_performance(n=n, n_tests=n_tests)  # type: ignore

    labels = ['MongoDB Insert', 'MongoDB Insert Many', 'Postgres Insert', 'Postgres Insert Many']
    plot_performance_comparison(
        title='MongoDB vs Postgres - Insert vs Insert Many Performance Comparison',
        results_list=[
            [insert_one_mean_std_mongo],
            [insert_many_mean_std_mongo],
            [insert_one_mean_std_pg],
            [insert_many_mean_std_pg],
        ],
        labels=labels,
        scaling_stages=[n]
    )

    plot_performance_comparison(
        title='MongoDB vs Postgres - Insert vs Insert Many Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(insert_one_mean_std_mongo, n),
            extrapolate_performance(insert_many_mean_std_mongo, n),
            extrapolate_performance(insert_one_mean_std_pg, n),
            extrapolate_performance(insert_many_mean_std_pg, n),
        ],
        labels=labels,
        scaling_stages=SCALING_STAGES
    )


def test_reads() -> None:
    n = 100
    n_tests = 1000
    read_mean_std_mongo: tuple[float, float] = mongo.test_read_performance(n=n, n_tests=n_tests)  # type: ignore
    read_mean_std_pg: tuple[float, float] = postgres.test_read_performance(n=n, n_tests=n_tests)  # type: ignore

    plot_performance_comparison(
        title='MongoDB vs Postgres - Read Performance Comparison',
        results_list=[
            [read_mean_std_mongo],
            [read_mean_std_pg]
        ],
        labels=['MongoDB Read', 'Postgres Read'],
        scaling_stages=[n]
    )

    scaling_stages = [250, 500, 1_000, 2_000]
    plot_performance_comparison(
        title='MongoDB vs Postgres - Read Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(read_mean_std_mongo, n),
            extrapolate_performance(read_mean_std_pg, n)
        ],
        labels=['MongoDB Read', 'Postgres Read'],
        scaling_stages=SCALING_STAGES
    )


def test_deletes() -> None:
    n = 100
    n_tests = 10_000
    delete_mean_std_mongo: tuple[float, float] = mongo.test_delete_performance(n=n, n_tests=n_tests)  # type: ignore
    delete_mean_std_pg: tuple[float, float] = postgres.test_delete_performance(n=n, n_tests=n_tests)  # type: ignore

    plot_performance_comparison(
        title='MongoDB vs Postgres - Delete Performance Comparison',
        results_list=[
            [delete_mean_std_mongo],
            [delete_mean_std_pg]
        ],
        labels=['MongoDB Delete', 'Postgres Delete'],
        scaling_stages=[n]
    )

    plot_performance_comparison(
        title='MongoDB vs Postgres - Delete Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(delete_mean_std_mongo, n),
            extrapolate_performance(delete_mean_std_pg,n)
        ],
        labels=['MongoDB Delete', 'Postgres Delete'],
        scaling_stages=SCALING_STAGES
    )


def test_updates() -> None:
    n = 100
    n_tests = 10_000
    update_mean_std_mongo: tuple[float, float] = mongo.test_update_performance(init_func_n=n, n_tests=n_tests)  # type: ignore
    update_mean_std_pg: tuple[float, float] = postgres.test_update_performance(init_func_n=n, n_tests=n_tests)  # type: ignore

    plot_performance_comparison(
        title='MongoDB vs Postgres - Update Performance Comparison',
        results_list=[
            [update_mean_std_mongo],
            [update_mean_std_pg]
        ],
        labels=['MongoDB Update', 'Postgres Update'],
        scaling_stages=[n]
    )

    plot_performance_comparison(
        title='MongoDB vs Postgres - Update Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(update_mean_std_mongo, n),
            extrapolate_performance(update_mean_std_pg, n)
        ],
        labels=['MongoDB Update', 'Postgres Update'],
        scaling_stages=SCALING_STAGES
    )

def test_reads_unique():
    n = 100
    n_tests = 1000
    read_mean_std_mongo: tuple[float, float] = mongo.test_unique_read_performance(n=n, n_tests=n_tests)  # type: ignore
    read_mean_std_pg: tuple[float, float] = postgres.test_read_performance(n=n, n_tests=n_tests)  # type: ignore

    plot_performance_comparison(
        title='MongoDB - with index vs Postgres - Read Performance Comparison',
        results_list=[
            [read_mean_std_mongo],
            [read_mean_std_pg]
        ],
        labels=['MongoDB Read', 'Postgres Read'],
        scaling_stages=[n]
    )

    plot_performance_comparison(
        title='MongoDB (with index) vs Postgres - Read Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(read_mean_std_mongo, n),
            extrapolate_performance(read_mean_std_pg, n)
        ],
        labels=['MongoDB Read', 'Postgres Read'],
        scaling_stages=SCALING_STAGES
    )


def test_insert_unique():
    n = 30
    n_tests = 1000
    insert_one_mean_std_mongo: tuple[float, float] = mongo.test_unique_insert_performance(n=n, n_tests=n_tests)  # type: ignore
    insert_one_mean_std_pg: tuple[float, float] = postgres.test_insert_performance(n=n, n_tests=n_tests)  # type: ignore

    labels = ['MongoDB Insert', 'Postgres Insert']
    plot_performance_comparison(
        title='MongoDB - with index vs Postgres - Insert Performance Comparison',
        results_list=[
            [insert_one_mean_std_mongo],
            [insert_one_mean_std_pg],
        ],
        labels=labels,
        scaling_stages=[n]
    )

    plot_performance_comparison(
        title='MongoDB (with index) vs Postgres - Insert Performance Comparison at Scale',
        results_list=[
            extrapolate_performance(insert_one_mean_std_mongo, n),
            extrapolate_performance(insert_one_mean_std_pg, n),
        ],
        labels=labels,
        scaling_stages=SCALING_STAGES
    )


if __name__ == "__main__":
    # test_inserts()
    # test_reads()
    # test_deletes()
    # test_updates()
    # test_reads_unique()
    test_insert_unique()