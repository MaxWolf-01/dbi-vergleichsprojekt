from pathlib import Path

import matplotlib.pyplot as plt
from typing import List

import numpy as np

PLOT_DIR: Path = Path(__file__).parent / 'plots'


def plot_performance_test(
        time_taken: List[float],
        scaling_stages: List[int],
        title: str,
) -> None:
    """
    Plots the performance test results and saves the plot to disk.

    :param time_taken: List of time measurements.
    :param scaling_stages: List of stages at which scaling occurs.
    :param title: Title of the plot.
    """
    plt.figure(figsize=(10, 6))
    labels = np.arange(len(scaling_stages))
    plt.bar(labels, time_taken, color='blue')
    plt.xlabel('Test Stages')
    plt.ylabel('Time Taken (seconds)')
    plt.title(title)
    plt.xticks(labels, [f'Stage {stage}' for stage in scaling_stages])

    PLOT_DIR.mkdir(exist_ok=True)
    filename = title.lower().replace(' ', '_')
    plt.savefig(PLOT_DIR / f'{filename}.png')
    plt.close()


def plot_performance_comparison(
        timings: List[List[float]],
        scaling_stages: List[int],
        labels: List[str],
        title: str,
) -> None:
    """
    Plots the comparison of multiple performance test results and saves the plot to disk.

    :param timings: List of lists of time measurements for each test.
    :param scaling_stages: List of stages at which scaling occurs.
    :param labels: List of labels for each test.
    :param title: Title of the plot.
    """
    n = len(scaling_stages)
    num_tests = len(timings)
    fig, ax = plt.subplots(figsize=(12, 6))

    # Calculate bar positions
    index = np.arange(n)
    bar_width = 0.35 / num_tests

    for i, (time_taken, label) in enumerate(zip(timings, labels)):
        bars = ax.bar(index + i * bar_width, time_taken, bar_width, label=label)
        # Annotate bars with their heights
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.2f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords='offset points',
                        ha='center', va='bottom')

    ax.set_xlabel('Test Stages')
    ax.set_ylabel('Time Taken (seconds)')
    ax.set_title(title)
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels([f'Stage {stage}' for stage in scaling_stages])
    ax.legend()

    plt.savefig(PLOT_DIR / f"{title.lower().replace(" ", "_")}.png")
    plt.close()
