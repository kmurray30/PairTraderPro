from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

MAX_DISPLAY_POINTS = 4000

ZOOM_UNDO_SCRIPT = """
(function() {
    var plotDiv = document.getElementById('ratio-chart');
    if (!plotDiv) return;

    var undoStack = [];
    var lastKnownView = null;
    var isUndo = false;

    function captureView(graphDiv) {
        var view = {};
        var xAxis = graphDiv.layout.xaxis || {};
        var yAxis = graphDiv.layout.yaxis || {};

        if (xAxis.autorange) {
            view['xaxis.autorange'] = true;
        } else if (xAxis.range) {
            view['xaxis.range[0]'] = xAxis.range[0];
            view['xaxis.range[1]'] = xAxis.range[1];
            view['xaxis.autorange'] = false;
        }

        if (yAxis.autorange) {
            view['yaxis.autorange'] = true;
        } else if (yAxis.range) {
            view['yaxis.range[0]'] = yAxis.range[0];
            view['yaxis.range[1]'] = yAxis.range[1];
            view['yaxis.autorange'] = false;
        }

        return view;
    }

    function undoZoom() {
        if (undoStack.length === 0) return;
        isUndo = true;
        var previousView = undoStack.pop();
        Plotly.relayout(plotDiv, previousView);
    }

    plotDiv.on('plotly_afterplot', function() {
        lastKnownView = captureView(plotDiv);
    });

    plotDiv.on('plotly_relayout', function() {
        if (isUndo) {
            isUndo = false;
            return;
        }
        if (lastKnownView) {
            undoStack.push(JSON.parse(JSON.stringify(lastKnownView)));
        }
    });

    document.addEventListener('keydown', function(event) {
        if ((event.ctrlKey || event.metaKey) && event.key === 'z') {
            event.preventDefault();
            undoZoom();
        }
    });

    var undoButton = document.createElement('button');
    undoButton.textContent = 'Undo zoom';
    undoButton.title = 'Undo zoom (Ctrl+Z / Cmd+Z)';
    undoButton.style.cssText = [
        'position:absolute',
        'top:88px',
        'left:170px',
        'z-index:1000',
        'padding:4px 10px',
        'font-size:12px',
        'cursor:pointer',
        'background:#fff',
        'border:1px solid #ccc',
        'border-radius:3px',
    ].join(';');
    undoButton.onclick = undoZoom;
    plotDiv.parentNode.style.position = 'relative';
    plotDiv.parentNode.appendChild(undoButton);
})();
"""


def compute_trailing_ratio_ma(ratio_series: pd.Series, window: int) -> pd.Series:
    """
    MA at index i uses ratio[i-window:i] (excludes i), matching the sim loop.
    """
    return ratio_series.rolling(window).mean().shift(1)


def add_ratio_chart_columns(
    stocks_df: pd.DataFrame,
    moving_average_window: int,
    trigger: float,
) -> pd.DataFrame:
    """Add ratio_ma and trigger band columns using the same math as the sim loop."""
    stocks_df = stocks_df.copy()
    stocks_df["ratio_ma"] = compute_trailing_ratio_ma(stocks_df["ratio"], moving_average_window)
    stocks_df["upper_threshold"] = stocks_df["ratio_ma"] * (1 + trigger)
    stocks_df["lower_threshold"] = stocks_df["ratio_ma"] * (1 - trigger)
    return stocks_df


def downsample_chart_data(
    chart_data: pd.DataFrame,
    trade_events: list[dict],
    max_points: int = MAX_DISPLAY_POINTS,
) -> pd.DataFrame:
    """
    Reduce point count for rendering while preserving shape via per-bucket endpoints.
    Trade timestamps are always kept so markers align with the ratio/threshold lines.
    """
    if len(chart_data) <= max_points and not trade_events:
        return chart_data.copy()

    sorted_data = chart_data.sort_values("timestamp").reset_index(drop=True)

    if len(sorted_data) > max_points:
        bucket_count = max(1, max_points // 2)
        bucket_size = len(sorted_data) / bucket_count
        selected_indices: set[int] = set()

        for bucket_index in range(bucket_count):
            start_index = int(bucket_index * bucket_size)
            end_index = int((bucket_index + 1) * bucket_size)
            if start_index >= len(sorted_data):
                break
            end_index = min(end_index, len(sorted_data))
            if start_index >= end_index:
                continue

            bucket = sorted_data.iloc[start_index:end_index]
            last_index = start_index + len(bucket) - 1
            selected_indices.add(start_index)
            selected_indices.add(last_index)
            selected_indices.add(int(bucket["ratio"].idxmin()))
            selected_indices.add(int(bucket["ratio"].idxmax()))

        downsampled = sorted_data.iloc[sorted(selected_indices)].reset_index(drop=True)
        if len(downsampled) > max_points:
            sample_indices = [
                int(index * (len(downsampled) - 1) / (max_points - 1))
                for index in range(max_points)
            ]
            downsampled = downsampled.iloc[sample_indices].reset_index(drop=True)
    else:
        downsampled = sorted_data.copy()

    if trade_events:
        trade_timestamps = {event["timestamp"] for event in trade_events}
        trade_rows = sorted_data[sorted_data["timestamp"].isin(trade_timestamps)]
        downsampled = (
            pd.concat([downsampled, trade_rows])
            .drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    return downsampled


def generate_ratio_chart(
    chart_data: pd.DataFrame,
    trade_events: list[dict],
    tickers: list[str],
    output_path: str,
    trigger_percent: float,
    date_range: tuple[str, str],
) -> str:
    """
    Write an interactive HTML chart of the stock ratio, moving average, trigger bands,
    and executed trade markers.
    """
    ratio_label = f"{tickers[0]}/{tickers[1]}"
    start_date, end_date = date_range
    title = f"{tickers[0]} / {tickers[1]} Ratio — {start_date} to {end_date} (trigger {trigger_percent}%)"
    subtitle = (
        "Scroll on chart to zoom time · Scroll on Y-axis to zoom vertically · "
        "Ctrl+Z to undo zoom · Hover trades for details"
    )

    display_data = downsample_chart_data(chart_data, trade_events)
    timestamps = pd.to_datetime(display_data["timestamp"])

    figure = go.Figure()

    line_trace_defaults = {
        "mode": "lines",
        "hoverinfo": "skip",
    }

    figure.add_trace(go.Scattergl(
        x=timestamps,
        y=display_data["ratio"],
        name=f"Ratio ({ratio_label})",
        line={"width": 1},
        **line_trace_defaults,
    ))

    figure.add_trace(go.Scattergl(
        x=timestamps,
        y=display_data["ratio_ma"],
        name="Moving average",
        line={"width": 1, "dash": "dash"},
        **line_trace_defaults,
    ))

    figure.add_trace(go.Scattergl(
        x=timestamps,
        y=display_data["upper_threshold"],
        name=f"Upper trigger (+{trigger_percent}%)",
        line={"width": 1, "dash": "dash", "color": "rgba(200, 80, 80, 0.7)"},
        **line_trace_defaults,
    ))

    figure.add_trace(go.Scattergl(
        x=timestamps,
        y=display_data["lower_threshold"],
        name=f"Lower trigger (-{trigger_percent}%)",
        line={"width": 1, "dash": "dash", "color": "rgba(80, 120, 200, 0.7)"},
        **line_trace_defaults,
    ))

    if trade_events:
        trade_timestamps = pd.to_datetime([event["timestamp"] for event in trade_events])
        trade_ratios = [event["ratio"] for event in trade_events]
        trade_directions = [event["direction"] for event in trade_events]
        trade_thresholds = [
            event["threshold"]
            for event in trade_events
        ]

        figure.add_trace(go.Scattergl(
            x=trade_timestamps,
            y=trade_ratios,
            mode="markers",
            name="Trades",
            marker={"symbol": "x", "size": 9, "color": "black", "line": {"width": 1}},
            customdata=list(zip(trade_directions, trade_thresholds)),
            hovertemplate=(
                "Time: %{x}<br>"
                "Ratio: %{y:.4f}<br>"
                "Threshold: %{customdata[1]:.4f}<br>"
                "Direction: %{customdata[0]}"
                "<extra></extra>"
            ),
        ))

    figure.update_layout(
        title={"text": f"{title}<br><sup>{subtitle}</sup>", "x": 0.5, "xanchor": "center"},
        xaxis_title="Time",
        yaxis_title=f"Ratio ({ratio_label})",
        hovermode="closest",
        dragmode="zoom",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.08, "xanchor": "right", "x": 1},
        margin={"l": 60, "r": 30, "t": 100, "b": 60},
        updatemenus=[
            {
                "type": "buttons",
                "direction": "left",
                "x": 0,
                "y": 1.18,
                "xanchor": "left",
                "yanchor": "top",
                "buttons": [
                    {
                        "label": "Reset Y",
                        "method": "relayout",
                        "args": [{"yaxis.autorange": True}],
                    },
                    {
                        "label": "Reset all",
                        "method": "relayout",
                        "args": [{"xaxis.autorange": True, "yaxis.autorange": True}],
                    },
                ],
            }
        ],
    )

    figure.update_xaxes(
        rangeslider={"visible": True, "thickness": 0.04},
        type="date",
        fixedrange=False,
    )

    figure.update_yaxes(
        fixedrange=False,
        autorange=True,
    )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    figure.write_html(
        str(output_file),
        include_plotlyjs="cdn",
        div_id="ratio-chart",
        post_script=ZOOM_UNDO_SCRIPT,
        config={
            "scrollZoom": True,
            "displayModeBar": True,
            "doubleClick": "reset+autosize",
        },
    )

    print(
        f"Chart rendered with {len(display_data):,} points "
        f"(downsampled from {len(chart_data):,}); {len(trade_events):,} trade markers"
    )

    return str(output_file.resolve())
