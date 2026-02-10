import re
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

# schedule_long を既に持ってるならこの行は不要
schedule = pd.read_excel("outputs/baseline_output.xlsx", sheet_name="schedule_long")


# ---- 日付順を安定化（D01..D30想定）----
def day_key(x):
    s = str(x)
    m = re.search(r"\d+", s)
    return int(m.group()) if m else 10**9


days = sorted(schedule["day"].unique(), key=day_key)

# ---- 編成順（A501..など）----
formations = sorted(schedule["formation_id"].astype(str).unique())

schedule["formation_id"] = schedule["formation_id"].astype(str)
schedule["day"] = schedule["day"].astype(str)

# ---- 表示用 ----
schedule["move"] = (
    schedule["op_start_loc"].astype(str) + "→" + schedule["op_end_loc"].astype(str)
)

# セル内に表示する文字：運用番号だけ（要求どおり）
schedule["cell_text"] = schedule["operation_id"].astype(str)

# もし「移動もセル内に2段で出したい」ならこっち
# schedule["cell_text"] = schedule["operation_id"].astype(str) + "<br>" + schedule["move"]

# ---- 色（終点ベース：必要なら start_loc に変える）----
station_colors = {
    "JB01": "#00bfff",  # 三鷹
    "JB07": "#9DC3E6",  # 中野
    "JB18": "#F4B084",  # 御茶ノ水
    "JB30": "#3cb371",  # 西船橋
    "JB33": "#7fffd4",  # 津田沼
    "JB35": "#9acd32",  # 幕張
    "JB39": "#8064A2",  # 千葉
    "OTHER": "#FFF2CC",  # その他
    "NA": "#D9D9D9",  # 欠損
}
start = schedule["op_start_loc"].astype("string")  # pandasのstring dtype（nanを保つ）
schedule["color_key"] = start.where(
    start.isin(list(station_colors.keys())), other="OTHER"
).fillna("NA")

# 数値化（NaNで落ちないように）
keys = list(station_colors.keys())  # この順番が色の順になる
key_to_int = {k: i for i, k in enumerate(keys)}

schedule["z"] = (
    schedule["color_key"]
    .map(key_to_int)
    .fillna(key_to_int["OTHER"])  # 念のため
    .astype(int)
)

# ---- 行列化（formation × day）----
z_mat = schedule.pivot(index="formation_id", columns="day", values="z").reindex(
    index=formations, columns=days
)
text_mat = schedule.pivot(
    index="formation_id", columns="day", values="cell_text"
).reindex(index=formations, columns=days)

# hoverに載せたい詳細（start_loc もここで入れる）
start_mat = schedule.pivot(
    index="formation_id", columns="day", values="op_start_loc"
).reindex(index=formations, columns=days)
end_mat = schedule.pivot(
    index="formation_id", columns="day", values="op_end_loc"
).reindex(index=formations, columns=days)
op_mat = schedule.pivot(
    index="formation_id", columns="day", values="operation_id"
).reindex(index=formations, columns=days)
dead_mat = schedule.pivot(
    index="formation_id", columns="day", values="deadhead"
).reindex(index=formations, columns=days)

# customdata: hovertemplateで参照するためにまとめる
# shape: (rows, cols, fields)
customdata = []
for f in formations:
    row = []
    for d in days:
        row.append(
            [
                op_mat.loc[f, d],
                start_mat.loc[f, d],
                end_mat.loc[f, d],
                dead_mat.loc[f, d],
            ]
        )
    customdata.append(row)


# ---- 離散カラースケール（5色）----
def discrete_colorscale(colors):
    n = len(colors)
    cs = []
    for i, c in enumerate(colors):
        v0 = i / n
        v1 = (i + 1) / n
        cs.append([v0, c])
        cs.append([v1, c])
    return cs


colors_in_order = [station_colors[k] for k in keys]
colorscale = discrete_colorscale(colors_in_order)

# ---- 図を作る ----
fig = go.Figure(
    data=go.Heatmap(
        z=z_mat.values,
        x=days,
        y=formations,
        colorscale=colorscale,
        zmin=-0.5,
        zmax=len(keys) - 0.5,
        showscale=False,  # 凡例不要ならFalse（欲しければTrueに）
        text=text_mat.values,  # セルに常時表示
        texttemplate="%{text}",
        textfont={"size": 10},
        xgap=1,  # 罫線っぽく見せる（隙間）
        ygap=1,
        customdata=customdata,
        hovertemplate=(
            "編成: %{y}<br>"
            "日: %{x}<br>"
            "運用: %{customdata[0]}<br>"
            "開始: %{customdata[1]}<br>"
            "終了: %{customdata[2]}<br>"
            "deadhead: %{customdata[3]}<extra></extra>"
        ),
    )
)

# deadhead=1 のセル座標を集める
dead_x = []
dead_y = []
for formation_id in formations:
    for day in days:
        v = dead_mat.loc[formation_id, day]
        if pd.notna(v) and int(v) == 1:
            dead_x.append(day)
            dead_y.append(formation_id)

# Heatmapの上に「赤枠」を重ねる（中身は透明）
fig.add_trace(
    go.Scatter(
        x=dead_x,
        y=dead_y,
        mode="markers",
        marker=dict(
            symbol="square",
            size=20,  # セルサイズに合わせて 18〜24 あたりで調整
            color="rgba(0,0,0,0)",  # 透明
            line=dict(color="red", width=2),
        ),
        hoverinfo="skip",
        showlegend=False,
    )
)


fig.update_layout(
    title="編成×日（セル内:運用番号 / hover:開始・終了）",
    xaxis_title="日",
    yaxis_title="編成",
    margin=dict(l=80, r=20, t=60, b=40),
)

# yは上からA501…にしたいならこのまま（formationsをソートしてる前提）
# 逆にしたいなら formations の並べ方を逆にするだけでOK

# ---- 縦スクロール：図を縦に長く作る → HTML側でスクロール枠に入れる ----
row_height_px = 18
fig_height = 120 + row_height_px * len(formations)
fig.update_layout(height=fig_height)

# HTMLラッパー（高さ固定＋縦スクロール）
plot_div = pio.to_html(fig, include_plotlyjs="cdn", full_html=False)
html = f"""
<!doctype html>
<html>
<head><meta charset="utf-8"></head>
<body>
  <div style="height:900px; overflow-y:auto; border:1px solid #ccc;">
    {plot_div}
  </div>
</body>
</html>
"""
with open("outputs/schedule_view_scroll.html", "w", encoding="utf-8") as f:
    f.write(html)

print("OK: schedule_view_scroll.html を開いて確認してね")
