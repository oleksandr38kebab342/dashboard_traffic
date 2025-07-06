import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from src.data_loader import load_dataset
from src.data_cleaner import clean_data
import ipaddress

# Визначаємо колонки з числовими даними для аналізу
NUMERIC_COLUMNS = ['dur', 'spkts', 'dpkts', 'sbytes', 'dbytes', 'rate', 
                   'sttl', 'dttl', 'sload', 'dload', 'sloss', 'dloss',
                   'sinpkt', 'dinpkt', 'sjit', 'djit', 'tcprtt', 'synack', 'ackdat']

def main():
    st.set_page_config(layout="wide", page_title="Аналіз мережевого трафіку")
    
    st.title("Інтерактивна панель аналізу мережевого трафіку")
    
    # Load and process data
    with st.spinner("Завантаження даних..."):
        df_real = load_dataset('real')
        df_synthetic = load_dataset()
        df = df_synthetic  
        df = clean_data(df, NUMERIC_COLUMNS)
    
    dataset_option = st.sidebar.selectbox(
        "Оберіть набір даних:",
        options=["Синтетичні дані", "Реальні дані"]
    )
    
    if dataset_option == "Реальні дані":
        df = df_real
    else:
        df = df_synthetic
    
    # Додамо обробку timestamp колонок
    if 'start_time' in df.columns:
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['hour'] = df['start_time'].dt.hour
        df['date'] = df['start_time'].dt.date
    
    # Sidebar for filters
    st.sidebar.header("Фільтри")
    
    protocols = ['Всі'] + sorted(df['proto'].unique().tolist())
    selected_protocol = st.sidebar.selectbox("Протокол:", protocols)
    
    services = ['Всі'] + sorted(df['service'].unique().tolist())
    selected_service = st.sidebar.selectbox("Сервіс:", services)
    
    states = ['Всі'] + sorted(df['state'].unique().tolist())
    selected_state = st.sidebar.selectbox("Стан:", states)
    
    # Filter by date range
    if 'date' in df.columns:
        date_min = df['date'].min()
        date_max = df['date'].max()
        selected_date_range = st.sidebar.date_input(
            "Діапазон дат:",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max
        )
        
        # Ensure we have both start and end dates
        if len(selected_date_range) == 2:
            start_date, end_date = selected_date_range
            date_mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        else:
            date_mask = df['date'] == selected_date_range[0]
    else:
        date_mask = pd.Series(True, index=df.index)
    
    # New filter specifically for anomalies
    st.sidebar.header("Фільтр аномалій")
    
    if 'anomaly' in df.columns:
        anomaly_options = ['Всі дані', 'Тільки нормальні', 'Тільки аномалії']
        anomaly_filter = st.sidebar.radio("Показати:", anomaly_options)
    else:
        anomaly_filter = 'Всі дані'
    
    # Apply filters to dataframe
    filtered_df = df.copy()
    
    if selected_protocol != 'Всі':
        filtered_df = filtered_df[filtered_df['proto'] == selected_protocol]
    
    if selected_service != 'Всі':
        filtered_df = filtered_df[filtered_df['service'] == selected_service]
    
    if selected_state != 'Всі':
        filtered_df = filtered_df[filtered_df['state'] == selected_state]
    
    # Apply date filter
    filtered_df = filtered_df[date_mask]
    
    # Apply anomaly filter
    if 'anomaly' in filtered_df.columns and anomaly_filter != 'Всі дані':
        if anomaly_filter == 'Тільки нормальні':
            filtered_df = filtered_df[filtered_df['anomaly'] == 0]
        else:  # 'Тільки аномалії'
            filtered_df = filtered_df[filtered_df['anomaly'] == 1]
    
    # Display basic statistics
    st.subheader("Основна статистика")
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Кількість сесій", f"{len(filtered_df):,}")
    col2.metric("Середня тривалість", f"{filtered_df['dur'].mean():.2f} с")
    col3.metric("Загальний обсяг даних", f"{filtered_df['sbytes'].sum():,} байтів")
    col4.metric("Найпоширеніший протокол", filtered_df['proto'].value_counts().index[0])
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Розподіли", "Кореляції", "Протоколи", 
                                                      "Загальний огляд", "Часовий аналіз", 
                                                      "Геовізуалізація", "Виявлення аномалій"])
    
    with tab1:
        st.subheader("Розподіл числових показників")
        column = st.selectbox("Оберіть показник:", options=NUMERIC_COLUMNS)
        fig = px.histogram(filtered_df, x=column, nbins=50, marginal="box", 
                           color='anomaly' if 'anomaly' in filtered_df.columns else None,
                           color_discrete_map={0: 'blue', 1: 'red'})
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Кореляційна матриця")
        corr = filtered_df[NUMERIC_COLUMNS].corr()
        fig = px.imshow(corr, text_auto=True, color_continuous_scale="RdBu_r")
        st.plotly_chart(fig, use_container_width=True)
        
    with tab3:
        st.subheader("Аналіз протоколів")
        col1, col2 = st.columns(2)
        
        with col1:
            proto_counts = filtered_df['proto'].value_counts()
            fig = px.pie(values=proto_counts.values, names=proto_counts.index, 
                         title="Розподіл протоколів")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            service_counts = filtered_df['service'].value_counts().head(10)
            fig = px.bar(x=service_counts.index, y=service_counts.values, 
                     title="Топ-10 сервісів", labels={'x': 'Сервіс', 'y': 'Кількість'})
            st.plotly_chart(fig, use_container_width=True)
        
    with tab4:
        st.subheader("Загальний огляд трафіку")
        
        # Scatter plot of bytes vs packets
        fig = px.scatter(filtered_df, x="spkts", y="sbytes", 
                         size="dur", color="proto", hover_name="service",
                         log_x=True, log_y=True, 
                         labels={"spkts": "Пакети", "sbytes": "Байти", "dur": "Тривалість"},
                         title="Співвідношення пакетів та байт за протоколами")
        st.plotly_chart(fig, use_container_width=True)
        
        # Boxplot of duration by protocol
        fig = px.box(filtered_df, x="proto", y="dur", 
                     color="proto", 
                     labels={"proto": "Протокол", "dur": "Тривалість (с)"},
                     title="Розподіл тривалості з'єднань за протоколами")
        st.plotly_chart(fig, use_container_width=True)
    
    with tab5:
        st.subheader("Часовий аналіз")
        
        if 'hour' in filtered_df.columns:
            # Hourly traffic volume
            hourly_traffic = filtered_df.groupby('hour')['sbytes'].sum().reset_index()
            fig = px.line(hourly_traffic, x='hour', y='sbytes', 
                          labels={'hour': 'Година доби', 'sbytes': 'Обсяг даних'},
                          title="Розподіл трафіку за годинами доби")
            st.plotly_chart(fig, use_container_width=True)
            
            # Теплова карта навантаження за днями тижня і годинами
            df['day_of_week'] = pd.to_datetime(df['start_time']).dt.dayofweek
            day_hour_traffic = df.groupby(['day_of_week', 'hour'])['sbytes'].sum().reset_index()
            day_hour_pivot = day_hour_traffic.pivot(index='day_of_week', columns='hour', values='sbytes')
            
            days = ['Понеділок', 'Вівторок', 'Середа', 'Четвер', "П'ятниця", 'Субота', 'Неділя']
            fig2 = px.imshow(day_hour_pivot, 
                            labels=dict(x="Година доби", y="День тижня", color="Обсяг даних"),
                            y=days,
                            color_continuous_scale="Viridis")
            st.plotly_chart(fig2, use_container_width=True)
            
            # Визначення піків навантаження
            peak_hours = hourly_traffic.nlargest(3, 'sbytes')
            st.subheader("Пікові години навантаження")
            st.write(f"Години з найбільшим трафіком: {', '.join(map(str, peak_hours['hour'].tolist()))}")
    
    with tab6:
        st.subheader("Географічна візуалізація трафіку")
        
        # Вибір напряму трафіку для аналізу
        traffic_direction = st.radio(
            "Напрямок трафіку:",
            options=["Джерело", "Призначення"], 
            horizontal=True
        )
        
        # Словник для перетворення українських назв країн в ISO коди
        country_iso_map = {
            'Україна': 'UKR',
            'США': 'USA',
            'Німеччина': 'DEU',
            'Польща': 'POL',
            'Франція': 'FRA',
            'Китай': 'CHN',
            'Велика Британія': 'GBR',
            'Канада': 'CAN',
            # Додані країни
            'Індія': 'IND',
            'Японія': 'JPN',
            'Австралія': 'AUS',
            'Бразилія': 'BRA',
            'Південна Корея': 'KOR',
            'Італія': 'ITA',
            'Іспанія': 'ESP',
            'Нідерланди': 'NLD',
            'Швеція': 'SWE',
            'Сінгапур': 'SGP',
            'Ізраїль': 'ISR',
            'ОАЕ': 'ARE'
        }
        
        country_col = 'src_country' if traffic_direction == "Джерело" else 'dst_country'
        
        # Агрегація даних за країнами
        country_traffic = filtered_df.groupby(country_col).agg(
            total_bytes=pd.NamedAgg(column='sbytes', aggfunc='sum'),
            count=pd.NamedAgg(column='id', aggfunc='count')
        ).reset_index()
        
        # Додаємо ISO коди для хороплету
        country_traffic['iso_alpha'] = country_traffic[country_col].map(country_iso_map)
        
        # Створюємо хороплет
        fig = px.choropleth(country_traffic, 
                           locations="iso_alpha",
                           color="total_bytes",
                           hover_name=country_col,
                           title=f"Розподіл трафіку за країнами ({traffic_direction.lower()})",
                           color_continuous_scale=px.colors.sequential.Plasma)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Таблиця з детальною інформацією
        st.subheader("Деталі трафіку за країнами")
        country_traffic['total_mb'] = country_traffic['total_bytes'] / (1024 * 1024)
        country_traffic = country_traffic.sort_values('total_bytes', ascending=False)
        st.dataframe(
            country_traffic.rename(columns={
                country_col: 'Країна',
                'count': 'Кількість з\'єднань',
                'total_bytes': 'Загальний обсяг (байти)',
                'total_mb': 'Загальний обсяг (МБ)'
            }).round(2)
        )
        
        # Додаємо аналіз характеристик трафіку за країнами
        st.subheader("Характеристики трафіку за країнами")
        
        metrics_options = ['sbytes', 'dbytes', 'spkts', 'dpkts', 'dur', 'sloss', 'dloss', 'sjit', 'djit']
        selected_metric = st.selectbox("Оберіть метрику для порівняння:", options=metrics_options)
        
        # Розрахунок середнього значення метрики за країнами
        country_metrics = filtered_df.groupby(country_col)[selected_metric].mean().reset_index()
        country_metrics = country_metrics.sort_values(selected_metric, ascending=False)
        
        # Горизонтальна діаграма для порівняння метрики за країнами
        fig = px.bar(country_metrics, 
                    x=selected_metric, 
                    y=country_col,
                    orientation='h',
                    title=f"Середнє значення {selected_metric} за країнами",
                    labels={country_col: "Країна", selected_metric: f"Середнє значення {selected_metric}"})
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Додаємо інтерактивну довідку про особливості трафіку різних країн
        st.info("""
        ### Особливості мережевого трафіку за країнами
        - **Індія**: Вищий обсяг даних (на 80% більше байтів)
        - **Китай**: Більша кількість пакетів
        - **США**: Триваліші з'єднання
        - **Бразилія**: Вища втрата пакетів
        - **Європейські країни**: Нижчий джитер (затримка)
        """)
        
    with tab7:
        st.subheader("Виявлення та аналіз аномалій")
        
        if 'anomaly' in df.columns:
            # Display overview of anomalies
            anomaly_counts = df['anomaly'].value_counts()
            total_connections = len(df)
            anomaly_percent = (anomaly_counts.get(1, 0) / total_connections) * 100 if total_connections > 0 else 0
            
            st.write(f"### Загальна статистика аномалій")
            st.write(f"Всього з'єднань: {total_connections}")
            st.write(f"Нормальні з'єднання: {anomaly_counts.get(0, 0)} ({100 - anomaly_percent:.2f}%)")
            st.write(f"Аномальні з'єднання: {anomaly_counts.get(1, 0)} ({anomaly_percent:.2f}%)")
            
            # Distribution of anomaly types
            if 'anomaly_type' in df.columns and anomaly_counts.get(1, 0) > 0:
                anomaly_types = df[df['anomaly'] == 1]['anomaly_type'].value_counts()
                
                fig = px.pie(
                    values=anomaly_types.values,
                    names=anomaly_types.index,
                    title="Розподіл типів аномалій"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Create detailed analysis by anomaly type
                st.write("### Детальний аналіз аномалій за типами")
                
                # Get unique anomaly types
                anomaly_type_options = ['Всі типи'] + sorted(df[df['anomaly'] == 1]['anomaly_type'].unique().tolist())
                selected_anomaly_type = st.selectbox("Оберіть тип аномалії для аналізу:", anomaly_type_options)
                
                # Filter data based on selected anomaly type
                if selected_anomaly_type != 'Всі типи':
                    anomaly_data = df[(df['anomaly'] == 1) & (df['anomaly_type'] == selected_anomaly_type)]
                else:
                    anomaly_data = df[df['anomaly'] == 1]
                
                # Calculate some statistics for the selected anomaly type
                if not anomaly_data.empty:
                    col1, col2, col3 = st.columns(3)
                    
                    col1.metric("Кількість з'єднань", len(anomaly_data))
                    col2.metric("Середня тривалість", f"{anomaly_data['dur'].mean():.4f} с")
                    col3.metric("Середній розмір", f"{int(anomaly_data['sbytes'].mean())} байт")
                    
                    # Compare normal vs anomaly characteristics
                    st.write("### Порівняння нормального та аномального трафіку")
                    
                    comparison_metric = st.selectbox(
                        "Оберіть метрику для порівняння:",
                        options=['pkt_to_byte_ratio', 'connection_rate', 'duration'] + NUMERIC_COLUMNS
                    )
                    
                    # Calculate metrics for comparison
                    if comparison_metric == 'pkt_to_byte_ratio':
                        normal_data = df[df['anomaly'] == 0].copy()
                        normal_data['pkt_to_byte_ratio'] = normal_data['spkts'] / (normal_data['sbytes'] + 1)
                        anomaly_data['pkt_to_byte_ratio'] = anomaly_data['spkts'] / (anomaly_data['sbytes'] + 1)
                        
                        fig = go.Figure()
                        fig.add_trace(go.Histogram(x=normal_data['pkt_to_byte_ratio'].clip(upper=0.1),
                                                name='Нормальний трафік', opacity=0.7,
                                                marker_color='blue'))
                        fig.add_trace(go.Histogram(x=anomaly_data['pkt_to_byte_ratio'].clip(upper=0.1),
                                                name='Аномальний трафік', opacity=0.7,
                                                marker_color='red'))
                        
                        fig.update_layout(title='Порівняння відношення пакетів до байтів',
                                        xaxis_title='Пакетів на байт (обмежено до 0.1)',
                                        yaxis_title='Кількість з\'єднань',
                                        barmode='overlay')
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif comparison_metric == 'connection_rate':
                        if 'start_time' in df.columns:
                            # Group connections by 5-minute intervals
                            df['time_window'] = df['start_time'].dt.floor('5min')
                            normal_rate = df[df['anomaly'] == 0].groupby('time_window').size()
                            anomaly_rate = df[df['anomaly'] == 1].groupby('time_window').size()
                            
                            # Combine into one dataframe for visualization
                            rate_df = pd.DataFrame({
                                'time': normal_rate.index,
                                'Нормальний трафік': normal_rate.values
                            })
                            
                            # Add anomaly rate if it exists for that time window
                            for time, count in anomaly_rate.items():
                                if time in rate_df['time'].values:
                                    rate_df.loc[rate_df['time'] == time, 'Аномальний трафік'] = count
                                else:
                                    new_row = pd.DataFrame({'time': [time], 'Нормальний трафік': [0], 'Аномальний трафік': [count]})
                                    rate_df = pd.concat([rate_df, new_row], ignore_index=True)
                            
                            rate_df = rate_df.fillna(0).sort_values('time')
                            
                            fig = px.line(rate_df, x='time', y=['Нормальний трафік', 'Аномальний трафік'],
                                        title='Інтенсивність з\'єднань з часом',
                                        labels={'value': 'Кількість з\'єднань', 'time': 'Час',
                                                'variable': 'Тип трафіку'})
                            st.plotly_chart(fig, use_container_width=True)
                    
                    elif comparison_metric == 'duration':
                        fig = go.Figure()
                        fig.add_trace(go.Box(y=df[df['anomaly'] == 0]['dur'].clip(upper=20),
                                            name='Нормальний трафік',
                                            marker_color='blue'))
                        fig.add_trace(go.Box(y=anomaly_data['dur'].clip(upper=20),
                                            name='Аномальний трафік',
                                            marker_color='red'))
                        fig.update_layout(title='Порівняння тривалості з\'єднань',
                                        yaxis_title='Тривалість (с, обмежено до 20с)')
                        st.plotly_chart(fig, use_container_width=True)
                    
                    else:  # For standard numeric metrics
                        fig = go.Figure()
                        fig.add_trace(go.Box(y=df[df['anomaly'] == 0][comparison_metric],
                                            name='Нормальний трафік',
                                            marker_color='blue'))
                        fig.add_trace(go.Box(y=anomaly_data[comparison_metric],
                                            name='Аномальний трафік',
                                            marker_color='red'))
                        fig.update_layout(title=f'Порівняння {comparison_metric}',
                                        yaxis_title=f'{comparison_metric}')
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Show examples of anomalies
                    st.write("### Приклади аномальних з'єднань")
                    sample_size = min(10, len(anomaly_data))
                    display_cols = ['start_time', 'proto', 'service', 'dur', 'spkts', 'sbytes', 'src_ip', 'dst_ip', 'anomaly_type']
                    st.dataframe(anomaly_data[display_cols].sample(sample_size))
            
            else:
                # Run anomaly detection if not already present
                st.write("### Виявлення аномалій в датасеті")
                st.write("У цьому датасеті відсутні мітки аномалій. Натисніть кнопку нижче, щоб запустити алгоритми виявлення аномалій:")
                
                if st.button("Виявити аномалії"):
                    with st.spinner("Аналіз даних і виявлення аномалій..."):
                        # Initialize anomaly column if it doesn't exist
                        if 'anomaly' not in df.columns:
                            df['anomaly'] = 0
                        if 'anomaly_type' not in df.columns:
                            df['anomaly_type'] = ''
                        
                        # 1. Detect DDoS attacks (existing code)
                        st.write("1. Виявлення атак DDoS...")
                        
                        # Group by destination IP and time window
                        df['time_window'] = pd.to_datetime(df['start_time']).dt.floor('10min')
                        ddos_threshold_pkt_rate = 1000  # Поріг для швидкості пакетів
                        
                        # Шукаємо з'єднання з надзвичайно високою швидкістю пакетів
                        ddos_mask = (df['spkts'] / df['dur'] > ddos_threshold_pkt_rate) & (df['dur'] < 0.1)
                        df.loc[ddos_mask, 'anomaly'] = 1
                        df.loc[ddos_mask, 'anomaly_type'] = 'ddos'
                        
                        # 2. Detect misconfigurations (enhanced)
                        st.write("2. Виявлення помилкових конфігурацій...")
                        
                        # a. Неправильні TTL значення
                        wrong_ttl_mask = ((df['sttl'] <= 2) | (df['sttl'] >= 254) | 
                                         (df['dttl'] <= 2) | (df['dttl'] >= 254))
                        df.loc[wrong_ttl_mask, 'anomaly'] = 1
                        df.loc[wrong_ttl_mask, 'anomaly_type'] = 'misconfig:wrong_ttl'
                        
                        # b. Нетипові розміри вікна TCP
                        window_size_mask = ((df['swin'] <= 3) | (df['swin'] >= 65534) | 
                                           (df['dwin'] <= 3) | (df['dwin'] >= 65534))
                        df.loc[window_size_mask, 'anomaly'] = 1
                        df.loc[window_size_mask, 'anomaly_type'] = 'misconfig:window_size'
                        
                        # c. Аномальне співвідношення пакетів до байтів
                        pkt_byte_ratio_mask = ((df['spkts'] > 100) & (df['sbytes'] / df['spkts'] < 10))
                        df.loc[pkt_byte_ratio_mask, 'anomaly'] = 1
                        df.loc[pkt_byte_ratio_mask, 'anomaly_type'] = 'misconfig:packet_size'
                        
                        # 3. Detect nonstandard ports
                        st.write("3. Виявлення нестандартних портів...")
                        
                        # Створюємо словник відповідності сервісів та стандартних портів
                        service_port_map = {
                            'http': 80, 'https': 443, 'dns': 53, 'ftp': 21, 'ssh': 22, 'smtp': 25, 
                            'pop3': 110, 'imap': 143, 'telnet': 23, 'ntp': 123, 'rdp': 3389
                        }
                        
                        # Перевіряємо порти на відповідність сервісу
                        for service, port in service_port_map.items():
                            nonstandard_port_mask = (df['service'] == service) & (df['dst_port'] != port)
                            df.loc[nonstandard_port_mask, 'anomaly'] = 1
                            df.loc[nonstandard_port_mask, 'anomaly_type'] = 'nonstandard_port'
                        
                        st.success(f"Аналіз завершено! Виявлено {df['anomaly'].sum()} аномалій.")
                        st.write(f"Розподіл аномалій за типами:")
                        
                        anomaly_counts = df[df['anomaly'] == 1]['anomaly_type'].value_counts()
                        st.write(anomaly_counts)
        else:
            st.write("Колонка 'anomaly' відсутня у датасеті. Для виявлення аномалій потрібно оновити структуру даних.")

if __name__ == "__main__":
    main()