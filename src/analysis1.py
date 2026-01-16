import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from pathlib import Path
    import polars as pl
    return Path, mo, pl


@app.cell
def _(Path):
    DATABASE_CLASS = Path("25-database-class/")
    return (DATABASE_CLASS,)


@app.cell
def _(DATABASE_CLASS, pl):
    history = pl.read_csv(DATABASE_CLASS / "history.csv")
    user = pl.read_csv(DATABASE_CLASS / "user.csv")
    spot = pl.read_csv(DATABASE_CLASS / "spot.csv")
    trip = pl.read_csv(DATABASE_CLASS / "trip.csv")

    dfs = {"history": history, "user": user, "spot": spot, "trip": trip}
    return dfs, history, user


@app.cell(hide_code=True)
def _(pl):
    def summarize_columns_wide(dfs: dict[str, pl.DataFrame]) -> pl.DataFrame:
        max_cols = max(len(df.columns) for df in dfs.values())

        rows = []
        for name, df in dfs.items():
            cols = df.columns
            row = {"dataset": name}
            for i in range(max_cols):
                row[f"col_{i + 1}"] = cols[i] if i < len(cols) else None
            rows.append(row)

        return pl.DataFrame(rows)
    return (summarize_columns_wide,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 扱うデータについて

    データはCSVファイルで配布されます。

    本講義の演習で扱うデータは以下の通りです。

    - 乗車履歴データ: history.csv
      - ユーザーが実際に車に乗車した履歴。
      - 1回の乗車がcsvの1行分のデータに相当します。
    - 駐車履歴データ: trip.csv
      - 車に乗車した際のユーザーの目的地。
      - 1回の乗車で複数の目的地へ行っている場合があります。
    - 目的地データ: spot.csv
      - 目的地に関する詳しい情報。
      - 駐車履歴データには目的地id(`spot_id`)のみが入っています。目的地に関する詳しい情報(店舗名や位置)は、このファイルから取得する必要があります。
    - ユーザー情報データ: user.csv
      - 全ユーザーのリストが入っています。
      - ユーザーは`staff`(教職員)と`student`(学生)の2種類あります。
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 各CSVのカラム

    ### history.csv

    |列名|内容|
    |---|---|
    |history_id|乗車履歴のid|
    |started_at|乗車し始めた時刻|
    |ended_at|乗車し終わった時刻|
    |from_parking_lot|乗車開始地点 (NAISTまたはATR)|
    |to_parking_lot|乗車終了地点 (NAISTまたはATR)|
    |car|使用した車の名前|
    |passengers_count|乗車人数（1の場合: 運転手のみ、2以上の場合: 運転手を含めた団体利用）※運転手同士の相乗りではない|
    |distance|移動距離|
    |user_id|車に乗車したユーザーのユーザーid|

    ### trip.csv

    |列名|内容|
    |---|---|
    |created_at|目的地へ行った日時|
    |lat|緯度|
    |lon|経度|
    |car|使用した車の名前|
    |user_id|目的地へ行ったユーザーのユーザーid|
    |spot_id|目的地id|
    |history_id|history.csv内の対応する行のhistory_id|

    ### spot.csv

    |列名|内容|
    |---|---|
    |spot_id|地点id|
    |spot_name|地点の名称|
    |lat|地点の緯度|
    |lon|地点の経度|
    |count|ユーザーが地点に行った回数|
    |spot_types|地点のタイプ (例: `restaurant`=レストラン) 関連する順に、複数のタイプが入っています|
    |is_parking|その場所がカーシェアの乗車/返却地点として用意された駐車場かどうか (Trueの場合カーシェアで用意された駐車場、Falseの場合カーシェアで用意された駐車場ではない)|

    ### user.csv

    |列名|内容|
    |---|---|
    |user_id|ユーザーid|
    |user_type|ユーザーが教職員(staff)か学生(student)か|
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ![image](https://github.com/piderlab/23-databese-class/assets/40050810/867f2e2a-0e8a-4c2e-ae2b-d29f23f89e1d)
    """)
    return


@app.cell(hide_code=True)
def _(dfs, mo, summarize_columns_wide):
    summary = summarize_columns_wide(dfs)
    summary

    mo.vstack(
        [
            mo.md("## カラム一覧表"),
            mo.ui.table(summary),
        ]
    )
    return


@app.cell
def _(history):
    history
    return


@app.cell
def _(history, pl, user):
    # started_at をDatetime化 + user_type 付与
    df = (
        history.with_columns(
            pl.col("started_at")
            .str.to_datetime(strict=False)
            .alias("started_at_dt"),
            pl.col("ended_at").str.to_datetime(strict=False).alias("ended_at_dt"),
        )
        .join(user, on="user_id", how="left")
        .with_columns(
            pl.col("started_at_dt").dt.weekday().alias("dow"),
            pl.col("started_at_dt").dt.hour().alias("hour"),
        )
        .drop_nulls(["user_type", "dow", "hour"])
    )


    agg = (
        df.group_by(["user_type", "dow", "hour"])
        .len()
        .rename({"len": "ride_count"})
    )

    agg_out = (
        agg.join(
            agg.group_by("user_type").agg(
                pl.col("ride_count").sum().alias("total")
            ),
            on="user_type",
            how="left",
        )
        .with_columns(
            (pl.col("ride_count") / pl.col("total")).alias("share_within_type")
        )
        .drop("total")
    )

    agg_out
    return agg, agg_out


@app.cell
def _(agg, agg_out, pl):
    import numpy as np

    # 0..6 x 0..23 の全組み合わせ
    grid = pl.DataFrame({"dow": list(range(7))}).join(
        pl.DataFrame({"hour": list(range(24))}), how="cross"
    )


    def heatmap_matrix(
        agg: pl.DataFrame, user_type: str, value_col: str = "share_within_type"
    ):
        # user_typeで絞る
        a = agg_out.filter(pl.col("user_type") == user_type).select(
            ["dow", "hour", value_col]
        )

        # 全グリッドに左結合して欠損を0埋め
        filled = (
            grid.join(a, on=["dow", "hour"], how="left")
            .with_columns(pl.col(value_col).fill_null(0.0))
            .sort(["dow", "hour"])
        )

        # pivot: rows=dow, cols=hour
        pivoted = filled.pivot(
            index="dow",
            on="hour",
            values=value_col,
            aggregate_function="first",
        ).sort("dow")

        # pivot後は "dow" 列 + 0..23 の列ができるので、行列にする
        mat = pivoted.drop("dow").to_numpy()
        return mat


    mat_staff = heatmap_matrix(agg, "staff", "share_within_type")
    mat_student = heatmap_matrix(agg, "student", "share_within_type")


    total_by_type = agg.group_by("user_type").agg(
        pl.col("ride_count").sum().alias("total_ride_count")
    )

    # dict にして引けるように
    total_map = dict(
        zip(
            total_by_type["user_type"].to_list(),
            total_by_type["total_ride_count"].to_list(),
        )
    )

    staff_total = int(total_map.get("staff", 0))
    student_total = int(total_map.get("student", 0))

    print(mat_staff.shape, mat_student.shape)  # (7, 24) になるはず
    return mat_staff, mat_student, np, staff_total, student_total


@app.cell(hide_code=True)
def _(mat_staff, np, staff_total):
    import matplotlib.pyplot as plt

    dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    hours = list(range(24))


    def plot_heatmap(mat: np.ndarray, title: str):
        fig, ax = plt.subplots(figsize=(12, 3.5))
        im = ax.imshow(mat, aspect="auto")
        ax.set_title(title)
        ax.set_yticks(range(7))
        ax.set_yticklabels(dow_labels)
        ax.set_xticks(range(24))
        ax.set_xticklabels(hours)
        ax.set_xlabel("Hour of day")
        ax.set_ylabel("Day of week")
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label("share_within_type")
        plt.tight_layout()


    plot_heatmap(
        mat_staff,
        f"staff: share within type by dow x hour (total rides={staff_total})",
    )
    plt.gca()
    return plot_heatmap, plt


@app.cell(hide_code=True)
def _(mat_student, plot_heatmap, plt, student_total):
    plot_heatmap(
        mat_student,
        f"student: share within type by dow x hour (total rides={student_total})",
    )
    plt.gca()
    return


@app.cell(hide_code=True)
def _(history, pl, plt, user):
    def box_abd_whiskey_plot_with_distance_and_time(value_col: str, title: str):
        df = (
            history.with_columns(
                pl.col("started_at")
                .str.to_datetime(strict=False)
                .alias("started_at_dt"),
                pl.col("ended_at")
                .str.to_datetime(strict=False)
                .alias("ended_at_dt"),
                pl.col("distance")
                .cast(pl.Float64, strict=False)
                .alias("distance_f"),
            )
            .join(user, on="user_id", how="left")
            .with_columns(
                # 所要時間（分）
                (pl.col("ended_at_dt") - pl.col("started_at_dt"))
                .dt.total_minutes()
                .alias("duration_min")
            )
            .drop_nulls(["user_type", "distance_f", "duration_min"])
            # 変な値を軽く除外（必要に応じて調整）
            .filter((pl.col("duration_min") > 0) & (pl.col("distance_f") >= 0))
        )

        def boxplot_by_type(value_col: str, title: str):
            staff_vals = (
                df.filter(pl.col("user_type") == "staff")
                .select(value_col)
                .to_numpy()
                .ravel()
            )
            student_vals = (
                df.filter(pl.col("user_type") == "student")
                .select(value_col)
                .to_numpy()
                .ravel()
            )

            fig, ax = plt.subplots(figsize=(6, 4))
            ax.boxplot(
                [staff_vals, student_vals],
                tick_labels=["staff", "student"],
                showfliers=True,
            )
            ax.set_title(title)
            ax.set_ylabel(value_col)
            plt.tight_layout()

        boxplot_by_type(value_col, title)
    return (box_abd_whiskey_plot_with_distance_and_time,)


@app.cell(hide_code=True)
def _(box_abd_whiskey_plot_with_distance_and_time, plt):
    box_abd_whiskey_plot_with_distance_and_time(
        "distance_f", "Distance by user_type (boxplot)"
    )
    plt.gca()
    return


@app.cell(hide_code=True)
def _(box_abd_whiskey_plot_with_distance_and_time, plt):
    box_abd_whiskey_plot_with_distance_and_time(
        "duration_min", "Duration(min) by user_type (boxplot)"
    )
    plt.gca()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
