#!/usr/bin/env python3
"""
Script to plot graphs with the event distribution and sum of events over time
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def plot_event_over_time(df):
    """
    Plots event count over time
    """
    _, ax = plt.subplots(1,1)
    for i in range(1, 7):
        df_filtered = df[df['type'] == i]
        ax.plot(df_filtered['start_time'], df_filtered['cumsum'].values/len(df), label=f'type {i}')
    ax.plot(df_filtered['start_time'], df_filtered['cumsum_total'].values/len(df), label=f'Total')
    ax.set_title("Events over time")
    ax.set_ylabel("Percentage of total events")
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_relative_event_distribution(df, scale_factor):
    """
    Plot a barplot with the percentages per event
    """
    _, ax = plt.subplots(1, 1)
    total_events = np.sum(df.groupby(['type']).size().values)
    bars = ax.bar(df.groupby(['type']).size().index, (df.groupby(['type']).size().values / total_events)*100)
    ax.grid(True)
    ax.bar_label(bars, labels=[f'{x:.2%}' for x in (df.groupby(['type']).size().values / total_events)])
    ax.set_xlabel('Event Type')
    ax.set_ylabel('Total Events (%)')
    ax.set_title(f"Distribution of update events {scale_factor.upper()}")
    plt.show()


def count_event_types(update_stream_csv_path):
    """
    Count the number of events in the updateStream file.
    We only use the first three columns as the other columns contain
    query information we do not need.
    """
    df = pd.read_csv(update_stream_csv_path, delimiter='|', usecols=range(3), header=None)
    df.rename(columns={0:'start_time', 1:'dependency_time', 2:'type'}, inplace=True)
    df['start_time'] = pd.to_datetime(df['start_time'], unit='ms')
    df['dependency_time'] = pd.to_datetime(df['dependency_time'], unit='ms')
    return df

if __name__ == "__main__":
    # Load the 
    scale_factor = 'sf10'
    data_dir = '/ldbc-data'
    df_forum  = count_event_types(f"{data_dir}/social_network-{scale_factor}-numpart-1/updateStream_0_0_forum.csv")
    df_person = count_event_types(f"{data_dir}/social_network-{scale_factor}-numpart-1/updateStream_0_0_person.csv")

    df = df_forum.append(df_person)
    df = df.sort_values(by='start_time')
    df['event_passed']=1
    df['cumsum'] = df.groupby('type')['event_passed'].transform(pd.Series.cumsum)
    df['cumsum_total'] = df['event_passed'].transform(pd.Series.cumsum)
    print(df.groupby(['type']).size().head())

    # # Barplot with relative counts
    plot_relative_event_distribution(df, scale_factor)

    # # Barplot with events over time
    plot_event_over_time(df)
