import streamlit as st
import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
import time
from datetime import datetime, timedelta, timezone

DREMIO_FLIGHT_HOST = 'localhost'
DREMIO_FLIGHT_PORT = 32010
DREMIO_USER = 'dremio'               
DREMIO_PASSWORD = 'dremio123'        
DREMIO_NESSIE_SOURCE = 'nessie_minio_catalog' 
DB_SCHEMA = 'ecommerce_db'

def query_dremio_flight(sql_query):
    """Thực thi truy vấn SQL trên Dremio sử dụng Arrow Flight."""
    df = pd.DataFrame()
    try:
        location = f"grpc://{DREMIO_FLIGHT_HOST}:{DREMIO_FLIGHT_PORT}"
        client = fl.connect(location)
        headers = [client.authenticate_basic_token(DREMIO_USER, DREMIO_PASSWORD)]
        options = fl.FlightCallOptions(headers=headers)
        flight_info = client.get_flight_info(fl.FlightDescriptor.for_command(sql_query), options)
        if not flight_info.endpoints:
            st.warning(f"Dremio did not return any endpoints for query: {sql_query[:100]}...")
            return df
        endpoint = flight_info.endpoints[0]
        reader = client.do_get(endpoint.ticket, options)
        arrow_table = reader.read_all()
        df = arrow_table.to_pandas()
    except pa.lib.ArrowIOError as e:
         if "authentication failed" in str(e).lower() or "invalid_argument" in str(e).lower():
             st.error(f"Lỗi xác thực Dremio Flight (kiểm tra user/pass): {e}")
         elif "unavailable" in str(e).lower() or "connection refused" in str(e).lower():
             st.error(f"Lỗi kết nối Dremio Flight (kiểm tra host/port, Dremio / Spark có chạy?): {e}")
         else:
             st.error(f"Lỗi kết nối hoặc I/O Dremio Flight: {e}")
    except TypeError as e:
        if "unexpected keyword argument 'timeout'" in str(e):
            st.error(f"Lỗi tương thích PyArrow: Phiên bản PyArrow của bạn không hỗ trợ tham số 'timeout'. Lỗi: {e}")
        else:
            st.error(f"Lỗi TypeError khi truy vấn Dremio Flight: {e}")
    except Exception as e:
        st.error(f"Lỗi không xác định khi truy vấn Dremio Flight: {e} - Query: {sql_query[:100]}...")
    finally:
        pass
    return df

st.set_page_config(layout="wide", page_title="Real-time Ecommerce Dashboard")
st.title("📊 Real-time E-commerce Dashboard")

REFRESH_INTERVAL_SECONDS = 10
placeholder = st.empty()

while True:
    with placeholder.container():
        now_utc = datetime.now(timezone.utc)
        last_update_time = now_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
        st.write(f"_Last updated: {last_update_time}_ (Refreshes every {REFRESH_INTERVAL_SECONDS} seconds)")

        st.subheader("🚀 Overview (Latest Metrics)")
        col1, col2, col3, col4 = st.columns(4)

        query_interval_minutes = 5
        query_start_time_dt = now_utc - timedelta(minutes=query_interval_minutes)
        query_start_time_str = query_start_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        threshold_caption = f">{query_start_time_str}" 

        query_orders_metric = f"""
        SELECT SUM(orders_count) as current_orders
        FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.orders_revenue_per_minute
        WHERE window_end > CAST('{query_start_time_str}' AS TIMESTAMP)
        """
        query_latest_order_ts = f"""
        SELECT MAX(window_end) as latest_ts FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.orders_revenue_per_minute
        """
        orders_metric_df = query_dremio_flight(query_orders_metric)
        latest_order_ts_df = query_dremio_flight(query_latest_order_ts)
        current_orders = 0
        latest_order_ts_str = "N/A"
        raw_order_sum_str = "N/A"
        if not orders_metric_df.empty:
            raw_value = orders_metric_df['current_orders'].iloc[0]
            if pd.notna(raw_value):
                current_orders = int(raw_value)
                raw_order_sum_str = str(current_orders)
            else: raw_order_sum_str = "NULL"
        if not latest_order_ts_df.empty and pd.notna(latest_order_ts_df['latest_ts'].iloc[0]):
            try:
                latest_order_ts = pd.to_datetime(latest_order_ts_df['latest_ts'].iloc[0])
                latest_order_ts_str = latest_order_ts.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e: latest_order_ts_str = str(latest_order_ts_df['latest_ts'].iloc[0])
        col1.metric(f"Orders (/{query_interval_minutes}min)", f"{current_orders}")
        col1.caption(f"Raw Sum: {raw_order_sum_str} | Latest Win End: {latest_order_ts_str} | Threshold: {threshold_caption}")

        query_revenue_metric = f"""
        SELECT SUM(total_revenue) as current_revenue
        FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.orders_revenue_per_minute
        WHERE window_end > CAST('{query_start_time_str}' AS TIMESTAMP)
        """
        revenue_metric_df = query_dremio_flight(query_revenue_metric)
        current_revenue = 0.0
        raw_revenue_sum_str = "N/A"
        if not revenue_metric_df.empty:
            raw_value = revenue_metric_df['current_revenue'].iloc[0]
            if pd.notna(raw_value):
                current_revenue = raw_value; raw_revenue_sum_str = f"{current_revenue:,.2f}"
            else: raw_revenue_sum_str = "NULL"
        col2.metric(f"Revenue (/{query_interval_minutes}min)", f"${current_revenue:,.2f}") 
        col2.caption(f"Raw Sum: {raw_revenue_sum_str} | Latest Win End: {latest_order_ts_str} | Threshold: {threshold_caption}")

        query_users_metric = f"""
        SELECT active_users_approx
        FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.active_users_per_window
        ORDER BY window_end DESC LIMIT 1
        """
        users_metric_df = query_dremio_flight(query_users_metric)
        current_users = 0
        if not users_metric_df.empty and pd.notna(users_metric_df['active_users_approx'].iloc[0]):
             current_users = int(users_metric_df['active_users_approx'].iloc[0])
        col3.metric("Active Users (5min window)", f"{current_users}")

        query_errors_metric = f"""
        SELECT SUM(error_count) as current_errors
        FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.errors_per_minute
        WHERE window_end > CAST('{query_start_time_str}' AS TIMESTAMP)
        """
        query_latest_error_ts = f"""
        SELECT MAX(window_end) as latest_ts FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.errors_per_minute
        """
        errors_metric_df = query_dremio_flight(query_errors_metric)
        latest_error_ts_df = query_dremio_flight(query_latest_error_ts)
        current_errors = 0
        latest_error_ts_str = "N/A"
        raw_error_sum_str = "N/A"
        if not errors_metric_df.empty:
             raw_value = errors_metric_df['current_errors'].iloc[0]
             if pd.notna(raw_value): current_errors = int(raw_value); raw_error_sum_str = str(current_errors)
             else: raw_error_sum_str = "NULL"
        if not latest_error_ts_df.empty and pd.notna(latest_error_ts_df['latest_ts'].iloc[0]):
            try:
                latest_error_ts = pd.to_datetime(latest_error_ts_df['latest_ts'].iloc[0])
                latest_error_ts_str = latest_error_ts.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e: latest_error_ts_str = str(latest_error_ts_df['latest_ts'].iloc[0])
        col4.metric(f"Errors (/{query_interval_minutes}min)", f"{current_errors}")
        col4.caption(f"Raw Sum: {raw_error_sum_str} | Latest Win End: {latest_error_ts_str} | Threshold: {threshold_caption}")


        st.divider()
        col5, col6 = st.columns([2, 1])
        with col5:
            st.subheader("📈 Order/Revenue Trend (Last 30 Mins)")
            thirty_mins_ago_dt = now_utc - timedelta(minutes=30)
            thirty_mins_ago_str = thirty_mins_ago_dt.strftime('%Y-%m-%d %H:%M:%S')
            query_trends = f"""
            SELECT window_start, window_end, orders_count, total_revenue
            FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.orders_revenue_per_minute
            WHERE window_start >= CAST('{thirty_mins_ago_str}' AS TIMESTAMP)
            ORDER BY window_start ASC
            """
            trends_df = query_dremio_flight(query_trends)
            if not trends_df.empty:
                 trends_df['window_start'] = pd.to_datetime(trends_df['window_start'])
                 trends_df = trends_df.set_index('window_start')
                 valid_trends = trends_df[['orders_count', 'total_revenue']].dropna()
                 if not valid_trends.empty:
                    st.line_chart(valid_trends)
                 else:
                    st.write("No valid trend data to plot yet.")
            else:
                 st.write("Not enough trend data yet.")

        with col6:
            st.subheader("🚨 Recent Alerts")
            ten_mins_ago_alert_dt = now_utc - timedelta(minutes=10)
            ten_mins_ago_alert_str = ten_mins_ago_alert_dt.strftime('%Y-%m-%d %H:%M:%S')
            query_alerts = f"""
            SELECT alert_type, description, alert_timestamp as alert_time
            FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.alerts
            WHERE alert_timestamp >= CAST('{ten_mins_ago_alert_str}' AS TIMESTAMP)
            ORDER BY alert_timestamp DESC LIMIT 5
            """
            alerts_df = query_dremio_flight(query_alerts)
            if not alerts_df.empty:
                alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time']).dt.strftime('%H:%M:%S')
                st.dataframe(alerts_df, hide_index=True, use_container_width=True)
            else:
                st.info("No recent alerts.")

        st.divider()
        col7, col8 = st.columns(2)
        with col7:
             st.subheader("📋 Latest Orders")
             query_latest_orders = f"""
             SELECT order_id, user_id, order_total, status, order_ts as order_time
             FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.orders_processed
             ORDER BY order_ts DESC LIMIT 10
             """
             latest_orders_df = query_dremio_flight(query_latest_orders)
             if not latest_orders_df.empty:
                 latest_orders_df['order_time'] = pd.to_datetime(latest_orders_df['order_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
                 latest_orders_df['order_total'] = latest_orders_df['order_total'].map('${:,.2f}'.format)
                 st.dataframe(latest_orders_df, hide_index=True, use_container_width=True)
             else:
                 st.write("No orders data yet.")

        with col8:
             st.subheader("🐞 Latest Errors")
             query_latest_errors = f"""
             SELECT service_name, severity, error_message, error_ts as error_time
             FROM {DREMIO_NESSIE_SOURCE}.{DB_SCHEMA}.errors_processed
             ORDER BY error_ts DESC LIMIT 10
             """
             latest_errors_df = query_dremio_flight(query_latest_errors)
             if not latest_errors_df.empty:
                 latest_errors_df['error_time'] = pd.to_datetime(latest_errors_df['error_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
                 st.dataframe(latest_errors_df, hide_index=True, use_container_width=True)
             else:
                 st.write("No errors data yet.")

        time.sleep(REFRESH_INTERVAL_SECONDS)