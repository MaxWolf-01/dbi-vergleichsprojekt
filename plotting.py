from pathlib import Path

import matplotlib.pyplot as plt
from typing import List, Optional, Tuple

import numpy as np

PLOT_DIR: Path = Path(__file__).parent / 'plots'


def plot_performance_comparison(
        results_list: List[List[tuple[float, float]]],
        scaling_stages: List[int],
        labels: List[str],
        title: str,
) -> None:
    """
    Plots the comparison of multiple performance test results with error bars and saves the plot to disk.

    :param results_list: List of lists of tuples of (mean time, std deviation) for each test.
    :param scaling_stages: List of stages at which scaling occurs.
    :param labels: List of labels for each test.
    :param title: Title of the plot.
    """
    n = len(scaling_stages)
    num_tests = len(results_list)
    fig, ax = plt.subplots(figsize=(12, 6))

    # Calculate bar positions
    index = np.arange(n)
    bar_width = 0.35 / num_tests

    for i, (results, label) in enumerate(zip(results_list, labels)):
        means, std_devs = zip(*results)
        bars = ax.bar(index + i * bar_width, means, yerr=std_devs, width=bar_width, label=label, capsize=5)
        # Annotate bars with their heights
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.2f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords='offset points',
                        ha='center', va='bottom')

    ax.set_xlabel('Number of operations')
    ax.set_ylabel('Time Taken (seconds)')
    ax.set_title(title)
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels([f'Stage {stage}' for stage in scaling_stages])
    ax.legend()

    plt.savefig(PLOT_DIR / f"{title.lower().replace(' ', '_')}.png")
    plt.close()
