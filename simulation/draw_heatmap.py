import plotly.graph_objects as go
from plotly.subplots import make_subplots

def draw_heatmap(df):
    for geo in df['geo'].unique():
        for type_name in df['type_name'].unique():
            for metric in [('percent_range_simulated_avg', 'percent_range_avg'),
                        ('distinct_bids_simulated_avg', 'distinct_bids_avg'),
                        ('NearestBid2Rec_simulated_avg', 'NearestBid2Rec_avg')]:
                # Создаем макет с 1 строкой и 2 столбцами
                fig = make_subplots(rows=1, cols=2, subplot_titles=[
                    f"{metric[0]}",
                    f"{metric[1]}"
                ], shared_yaxes=True)

                df_temp = df[(df['geo'] == geo) & (df['type_name'] == type_name)]

                # Первая тепловая карта
                heatmap1 = go.Heatmap(
                    x=df_temp['eta_bin'],
                    y=df_temp['AtoB_seconds_bin'],
                    z=df_temp[metric[0]],
                    colorscale='RdBu',
                    zmid=0,
                    colorbar=dict(title="Value", len=0.8)
                )

                # Вторая тепловая карта
                heatmap2 = go.Heatmap(
                    x=df_temp['eta_bin'],
                    y=df_temp['AtoB_seconds_bin'],
                    z=df_temp[metric[1]],
                    colorscale='RdBu',
                    zmid=0,
                    colorbar=dict(title="Value", len=0.8, x=1.02)  # Переместим цветовую шкалу
                )

                # Добавляем графики в макет
                fig.add_trace(heatmap1, row=1, col=1)
                fig.add_trace(heatmap2, row=1, col=2)

                # Настраиваем макет
                fig.update_layout(
                    title_text=str(geo) + ', ' + str(type_name),
                    width=1200,
                    height=600,
                    template='plotly_white',
                    xaxis_title="ETA (seconds)",
                    yaxis_title="AtoB Seconds",
                    xaxis2_title="ETA (seconds)"
                )

                # Показываем график
                fig.show()
