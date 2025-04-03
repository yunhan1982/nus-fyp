import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta

from bitemporal_space import BitemporalSpace, INFINITY


def plot_bitemporal_space(space: BitemporalSpace, title: str = "Bitemporal Space"):
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Convert datetime to matplotlib-compatible format
    one_day = timedelta(days=1)
    
    tt_min = min(r.tt_from for r in space.rects) - one_day
    print(space.rects)
    tt_max = one_day + max(r.tt_to for r in space.rects if r.tt_to != INFINITY) or space.latest_tx_time
    vt_min = min(r.vt_from for r in space.rects) - one_day
    vt_max = one_day + max(r.vt_to for r in space.rects if r.vt_to != INFINITY) or max(r.vt_from for r in space.rects)

    # Use a colormap for distinct colors
    colors = plt.cm.Set3(np.linspace(0, 1, len(space.rects)))

    for i, rect in enumerate(space.rects):
        # Convert times to matplotlib dates
        tt_start = mdates.date2num(rect.tt_from)
        tt_end = mdates.date2num(rect.tt_to if rect.tt_to != INFINITY else tt_max)
        vt_start = mdates.date2num(rect.vt_from)
        vt_end = mdates.date2num(rect.vt_to if rect.vt_to != INFINITY else vt_max)
        
        # Plot rectangle without border
        ax.fill_between(
            [tt_start, tt_end], 
            [vt_start, vt_start], 
            [vt_end, vt_end], 
            color=colors[i], 
            alpha=0.6, 
            edgecolor='none',
            label=f"Index {rect.index} (Age: {rect.data['age']})"
        )  

    # Format axes
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.yaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.set_xlabel("Transaction Time (TT)")
    ax.set_ylabel("Valid Time (VT)")
    ax.set_title(title)
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Set the limits for the axes using the calculated min and max values
    ax.set_xlim(tt_min, tt_max + one_day)
    ax.set_ylim(vt_min, vt_max + one_day)

    plt.show()