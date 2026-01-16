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

    dfs = {
        "history": history,
        "user": user,
        "spot": spot,
        "trip": trip
    }
    return dfs, history, spot, trip, user


@app.cell(hide_code=True)
def _(pl):
    def summarize_columns_wide(dfs: dict[str, pl.DataFrame]) -> pl.DataFrame:
        max_cols = max(len(df.columns) for df in dfs.values())

        rows = []
        for name, df in dfs.items():
            cols = df.columns
            row = {"dataset": name}
            for i in range(max_cols):
                row[f"col_{i+1}"] = cols[i] if i < len(cols) else None
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

    mo.vstack([
      mo.md("## カラム一覧表"),
      mo.ui.table(summary),
    ])
    return


@app.cell
def _(history, spot, trip, user):
    history_with_user = history.join(user, on="user_id", how="left")
    trip_with_user = trip.join(user, on="user_id", how="left")
    spot_with_trip = spot.join(trip, on="spot_id", how="left")

    return


if __name__ == "__main__":
    app.run()
