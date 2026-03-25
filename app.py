import streamlit as st
import anthropic
import pandas as pd
import json
import re
import plotly.express as px
from datetime import datetime
from supabase import create_client

st.set_page_config(page_title="DataWarehouse Chat", page_icon="🏢", layout="wide")

st.markdown("""
<style>
.stApp { background-color: #0f0f1a; color: #e0e0e0; }
</style>
""", unsafe_allow_html=True)

SCHEMA_CONTEXT = """
You are a SQL expert. The database has two schemas: 'sales' and 'customers'.

SCHEMA: sales
  TABLE: sales.products
    - product_id (int, PK)
    - product_name (varchar)
    - category (varchar) -- Electronics, Accessories, Furniture
    - unit_price (numeric)
    - cost_price (numeric)

  TABLE: sales.orders
    - order_id (int, PK)
    - customer_id (int, FK -> customers.profiles.customer_id)
    - product_id (int, FK -> sales.products.product_id)
    - order_date (date)
    - quantity (int)
    - total_amount (numeric)
    - region (varchar) -- Asia, North America, Middle East, Europe
    - sales_rep (varchar)
    - status (varchar)

SCHEMA: customers
  TABLE: customers.profiles
    - customer_id (int, PK)
    - full_name (varchar)
    - email (varchar)
    - country (varchar)
    - city (varchar)
    - age (int)
    - gender (varchar)
    - signup_date (date)
    - segment (varchar) -- Premium, Standard, Enterprise

  TABLE: customers.interactions
    - interaction_id (int, PK)
    - customer_id (int, FK -> customers.profiles.customer_id)
    - interaction_date (date)
    - channel (varchar) -- Email, Chat, Phone
    - type (varchar) -- Support, Inquiry, Complaint
    - satisfaction_score (int, 1-5)

RULES:
- Always use schema-qualified table names
- Return ONLY raw SQL, no explanation, no markdown, no backticks
- Limit to 500 rows unless aggregation
"""

def get_supabase():
    url = "https://eheccrezcqanmlwvujds.supabase.co"
    key = st.secrets.get("SUPABASE_KEY", "")
    return create_client(url, key)

def run_query(sql):
    sb = get_supabase()
    result = sb.rpc("run_sql", {"query": sql}).execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame()

def generate_sql(question, api_key):
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": f"{SCHEMA_CONTEXT}\n\nQuestion: {question}\n\nSQL only:"}]
    )
    sql = msg.content[0].text.strip()
    sql = re.sub(r"```sql|```", "", sql).strip()
    sql = sql.rstrip(";").strip()
    return sql

def generate_summary(question, df, api_key):
    client = anthropic.Anthropic(api_key=api_key)
    sample = df.head(3).to_dict(orient="records")
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=200,
        messages=[{"role": "user", "content": f"User asked: '{question}'\n{len(df)} rows returned.\nSample: {json.dumps(sample, default=str)}\nWrite a clear 1-2 sentence answer."}]
    )
    return msg.content[0].text.strip()

def suggest_chart(df, api_key):
    if df.empty or len(df.columns) < 2:
        return "table"
    client = anthropic.Anthropic(api_key=api_key)
    dtypes = {c: str(df[c].dtype) for c in df.columns}
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=10,
        messages=[{"role": "user", "content": f"Column types: {dtypes}\nReply with ONE word only: bar, line, pie, scatter, or table"}]
    )
    return msg.content[0].text.strip().lower()

def render_chart(df, chart_type, title=""):
    if df.empty:
        return None
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric = [c for c in df.columns if c not in numeric_cols]
    try:
        if chart_type == "bar" and numeric_cols and non_numeric:
            fig = px.bar(df, x=non_numeric[0], y=numeric_cols[0], title=title, template="plotly_dark", color_discrete_sequence=["#6366f1"])
        elif chart_type == "line" and numeric_cols:
            x = non_numeric[0] if non_numeric else df.columns[0]
            fig = px.line(df, x=x, y=numeric_cols[0], title=title, template="plotly_dark", color_discrete_sequence=["#6366f1"])
        elif chart_type == "pie" and numeric_cols and non_numeric:
            fig = px.pie(df, names=non_numeric[0], values=numeric_cols[0], title=title, template="plotly_dark")
        elif chart_type == "scatter" and len(numeric_cols) >= 2:
            fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title=title, template="plotly_dark", color_discrete_sequence=["#6366f1"])
        else:
            return None
        fig.update_layout(paper_bgcolor="#0f0f1a", plot_bgcolor="#1e1e2e", font_color="#e0e0e0")
        return fig
    except:
        return None

if "messages" not in st.session_state:
    st.session_state.messages = []
if "saved_graphs" not in st.session_state:
    st.session_state.saved_graphs = []

with st.sidebar:
    st.markdown("## 🏢 DataWarehouse")
    st.markdown("---")
    api_key = st.text_input("Anthropic API Key", type="password", placeholder="sk-ant-...")
    st.markdown("---")
    page = st.radio("Navigation", ["💬 Chat", "📊 Saved Graphs", "🗄️ Schema"])
    st.markdown("---")
    st.markdown("### 💡 Try asking:")
    st.markdown("""
- Total sales by region
- Top 5 customers by spend
- Monthly revenue trend
- Products by category
- Average satisfaction by channel
- Sales rep performance
    """)
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()

if page == "💬 Chat":
    st.markdown("## 💬 Chat with your Data")

    for msg in st.session_state.messages:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(f"**📋** {msg['summary']}")
                with st.expander("🔍 SQL"):
                    st.code(msg["sql"], language="sql")
                if msg.get("df"):
                    df = pd.DataFrame(msg["df"])
                    if not df.empty:
                        if msg.get("chart_type", "table") != "table":
                            fig = render_chart(df, msg["chart_type"], msg.get("question",""))
                            if fig:
                                st.plotly_chart(fig, use_container_width=True)
                        with st.expander(f"📄 Data ({len(df)} rows)"):
                            st.dataframe(df, use_container_width=True)
                        if st.button("💾 Save", key=f"s_{msg.get('id','')}"):
                            st.session_state.saved_graphs.append({
                                "id": msg.get("id",""),
                                "title": msg.get("question","")[:50],
                                "question": msg.get("question",""),
                                "sql": msg["sql"],
                                "chart_type": msg.get("chart_type","table"),
                                "df": msg["df"],
                                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                            })
                            st.success("Saved!")

    if prompt := st.chat_input("Ask about your data..."):
        if not api_key:
            st.error("Enter your Anthropic API key in the sidebar!")
        else:
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        sql = generate_sql(prompt, api_key)
                        df = run_query(sql)
                        summary = generate_summary(prompt, df, api_key)
                        chart_type = suggest_chart(df, api_key)
                        st.markdown(f"**📋** {summary}")
                        with st.expander("🔍 SQL"):
                            st.code(sql, language="sql")
                        if not df.empty:
                            if chart_type != "table":
                                fig = render_chart(df, chart_type, prompt)
                                if fig:
                                    st.plotly_chart(fig, use_container_width=True)
                            with st.expander(f"📄 Data ({len(df)} rows)"):
                                st.dataframe(df, use_container_width=True)
                        msg_id = str(datetime.now().timestamp())
                        st.session_state.messages.append({
                            "role": "assistant", "id": msg_id,
                            "question": prompt, "sql": sql,
                            "summary": summary, "chart_type": chart_type,
                            "df": df.to_dict(orient="records") if not df.empty else []
                        })
                        if st.button("💾 Save Graph", key=f"sn_{msg_id}"):
                            st.session_state.saved_graphs.append({
                                "id": msg_id, "title": prompt[:50],
                                "question": prompt, "sql": sql,
                                "chart_type": chart_type,
                                "df": df.to_dict(orient="records"),
                                "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                            })
                            st.success("✅ Saved!")
                    except Exception as e:
                        st.error(f"Error: {e}")

elif page == "📊 Saved Graphs":
    st.markdown("## 📊 Saved Graphs")
    if not st.session_state.saved_graphs:
        st.info("No saved graphs yet. Chat with your data and click 'Save Graph'!")
    else:
        for i, g in enumerate(st.session_state.saved_graphs):
            with st.expander(f"📊 {g['title']} — {g.get('saved_at','')}"):
                col1, col2 = st.columns([3,1])
                with col2:
                    if st.button("🗑️ Delete", key=f"d_{i}"):
                        st.session_state.saved_graphs.pop(i)
                        st.rerun()
                df = pd.DataFrame(g["df"])
                if not df.empty:
                    if g["chart_type"] != "table":
                        fig = render_chart(df, g["chart_type"], g["title"])
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                    with st.expander("🔍 SQL"):
                        st.code(g["sql"], language="sql")
                    st.markdown("#### 📥 Download")
                    cols = st.multiselect("Select columns", list(df.columns), default=list(df.columns), key=f"c_{i}")
                    if cols:
                        st.download_button("⬇️ Download CSV", df[cols].to_csv(index=False),
                            file_name=f"{g['title'][:30].replace(' ','_')}.csv",
                            mime="text/csv", key=f"dl_{i}")
                        st.dataframe(df[cols], use_container_width=True)

elif page == "🗄️ Schema":
    st.markdown("## 🗄️ Schema Explorer")
    sb = get_supabase()
    result = sb.rpc("run_sql", {"query": """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN ('sales','customers')
        ORDER BY table_schema, table_name, ordinal_position
    """}).execute()
    if result.data:
        df = pd.DataFrame(result.data)
        for schema in ["sales","customers"]:
            st.markdown(f"### 📁 `{schema}`")
            sdf = df[df["table_schema"]==schema]
            for table in sdf["table_name"].unique():
                tdf = sdf[sdf["table_name"]==table][["column_name","data_type"]]
                with st.expander(f"📋 {schema}.{table}"):
                    st.dataframe(tdf, use_container_width=True, hide_index=True)
