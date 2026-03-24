import streamlit as st
import psycopg2
import psycopg2.extras
import anthropic
import pandas as pd
import json
import re
import plotly.express as px
from datetime import datetime

# ─── Page Config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="DataWarehouse Chat",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Custom CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0f0f1a; }
    .main .block-container { padding-top: 2rem; }
    .chat-message { padding: 1rem; border-radius: 10px; margin-bottom: 1rem; }
    .sql-box { background: #1e1e2e; border: 1px solid #3d3d5c; border-radius: 8px; padding: 1rem; font-family: monospace; font-size: 13px; color: #a0a0c0; }
    .summary-box { background: #0d2818; border: 1px solid #166534; border-radius: 8px; padding: 1rem; color: #4ade80; }
    .metric-card { background: #1e1e2e; border: 1px solid #3d3d5c; border-radius: 10px; padding: 1rem; text-align: center; }
</style>
""", unsafe_allow_html=True)

# ─── Schema Context ───────────────────────────────────────────────
SCHEMA_CONTEXT = """
You are a SQL expert. The database has two schemas: 'sales' and 'customers'.

SCHEMA: sales
  TABLE: sales.products
    - product_id (int, PK)
    - product_name (varchar)
    - category (varchar)  -- values: Electronics, Accessories, Furniture
    - unit_price (numeric)
    - cost_price (numeric)

  TABLE: sales.orders
    - order_id (int, PK)
    - customer_id (int, FK -> customers.profiles.customer_id)
    - product_id (int, FK -> sales.products.product_id)
    - order_date (date)
    - quantity (int)
    - total_amount (numeric)
    - region (varchar)  -- values: Asia, North America, Middle East, Europe
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
    - segment (varchar)  -- values: Premium, Standard, Enterprise

  TABLE: customers.interactions
    - interaction_id (int, PK)
    - customer_id (int, FK -> customers.profiles.customer_id)
    - interaction_date (date)
    - channel (varchar)  -- values: Email, Chat, Phone
    - type (varchar)     -- values: Support, Inquiry, Complaint
    - satisfaction_score (int, 1-5)

RULES:
- Always use schema-qualified table names (e.g. sales.orders, customers.profiles)
- For joins across schemas, use the customer_id foreign key
- Return ONLY the raw SQL query, no explanation, no markdown, no backticks
- Limit results to 500 rows unless asked for aggregation
"""

# ─── DB Connection ────────────────────────────────────────────────
def get_connection():
    db_password = st.secrets.get("DB_PASSWORD", "Amorodio.123")
    conn_string = f"postgresql://postgres.eheccrezcqanmlwvujds:{db_password}@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres"
    return psycopg2.connect(conn_string)

def run_query(sql):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame([dict(r) for r in rows])

# ─── AI Functions ─────────────────────────────────────────────────
def generate_sql(question, api_key):
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": f"{SCHEMA_CONTEXT}\n\nUser question: {question}\n\nGenerate only the SQL query:"}]
    )
    sql = msg.content[0].text.strip()
    return re.sub(r"```sql|```", "", sql).strip()

def generate_summary(question, df, api_key):
    client = anthropic.Anthropic(api_key=api_key)
    sample = df.head(3).to_dict(orient="records")
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=200,
        messages=[{"role": "user", "content": f"User asked: '{question}'\nQuery returned {len(df)} rows.\nSample: {json.dumps(sample, default=str)}\nWrite a clear 1-2 sentence plain English answer."}]
    )
    return msg.content[0].text.strip()

def suggest_chart(df, api_key):
    if df.empty or len(df.columns) < 2:
        return "table"
    client = anthropic.Anthropic(api_key=api_key)
    cols = list(df.columns)
    dtypes = {c: str(df[c].dtype) for c in cols}
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=50,
        messages=[{"role": "user", "content": f"Columns and types: {dtypes}\nFirst row: {df.head(1).to_dict(orient='records')}\nReply with ONLY one word: bar, line, pie, scatter, or table"}]
    )
    return msg.content[0].text.strip().lower()

# ─── Chart Renderer ───────────────────────────────────────────────
def render_chart(df, chart_type, title=""):
    if df.empty:
        st.warning("No data to chart.")
        return None

    cols = list(df.columns)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric = [c for c in cols if c not in numeric_cols]

    try:
        if chart_type == "bar" and len(numeric_cols) >= 1 and len(non_numeric) >= 1:
            fig = px.bar(df, x=non_numeric[0], y=numeric_cols[0], title=title,
                        color_discrete_sequence=["#6366f1"],
                        template="plotly_dark")
        elif chart_type == "line" and len(numeric_cols) >= 1:
            x_col = non_numeric[0] if non_numeric else cols[0]
            fig = px.line(df, x=x_col, y=numeric_cols[0], title=title,
                         color_discrete_sequence=["#6366f1"],
                         template="plotly_dark")
        elif chart_type == "pie" and len(numeric_cols) >= 1 and len(non_numeric) >= 1:
            fig = px.pie(df, names=non_numeric[0], values=numeric_cols[0], title=title,
                        template="plotly_dark")
        elif chart_type == "scatter" and len(numeric_cols) >= 2:
            fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], title=title,
                           color_discrete_sequence=["#6366f1"],
                           template="plotly_dark")
        else:
            return None

        fig.update_layout(
            paper_bgcolor="#0f0f1a",
            plot_bgcolor="#1e1e2e",
            font_color="#e0e0e0",
            title_font_size=16
        )
        return fig
    except Exception:
        return None

# ─── Session State ────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "saved_graphs" not in st.session_state:
    st.session_state.saved_graphs = []

# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏢 DataWarehouse")
    st.markdown("---")

    api_key = st.text_input(
        "Anthropic API Key",
        value="",
        type="password",
        help="Your Anthropic API key"
    )

    st.markdown("---")
    page = st.radio("Navigation", ["💬 Chat", "📊 Saved Graphs", "🗄️ Schema Explorer"])

    st.markdown("---")
    st.markdown("### 💡 Try asking:")
    st.markdown("""
- Total sales by region
- Top 5 customers by spend
- Monthly revenue trend
- Products by category
- Average satisfaction by channel
- Customers from India
- Sales rep performance
    """)

    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()

# ─── PAGE: Chat ───────────────────────────────────────────────────
if page == "💬 Chat":
    st.markdown("## 💬 Chat with your Data")
    st.markdown("Ask questions in plain English — I'll write the SQL and show you the results.")

    # Display chat history
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(f"**📋 Summary:** {msg['summary']}")
                with st.expander("🔍 View SQL"):
                    st.code(msg["sql"], language="sql")
                if msg.get("df") is not None:
                    df = pd.DataFrame(msg["df"])
                    if not df.empty:
                        # Chart
                        if msg.get("chart_type") and msg["chart_type"] != "table":
                            fig = render_chart(df, msg["chart_type"], msg.get("question", ""))
                            if fig:
                                st.plotly_chart(fig, use_container_width=True)

                        # Data table
                        with st.expander(f"📄 View Data ({len(df)} rows)"):
                            st.dataframe(df, use_container_width=True)

                        # Save graph button
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            if st.button("💾 Save Graph", key=f"save_{msg.get('id','')}"):
                                st.session_state.saved_graphs.append({
                                    "id": str(datetime.now().timestamp()),
                                    "title": msg.get("question", "Untitled")[:50],
                                    "question": msg.get("question", ""),
                                    "sql": msg["sql"],
                                    "chart_type": msg.get("chart_type", "table"),
                                    "df": msg["df"],
                                    "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                                })
                                st.success("Saved!")

    # Chat input
    if prompt := st.chat_input("Ask about your data..."):
        if not api_key:
            st.error("Please enter your Anthropic API key in the sidebar.")
        else:
            # Add user message
            st.session_state.messages.append({"role": "user", "content": prompt})

            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        # Generate SQL
                        sql = generate_sql(prompt, api_key)

                        # Run query
                        df = run_query(sql)

                        # Generate summary
                        summary = generate_summary(prompt, df, api_key)

                        # Suggest chart
                        chart_type = suggest_chart(df, api_key)

                        # Display
                        st.markdown(f"**📋 Summary:** {summary}")
                        with st.expander("🔍 View SQL"):
                            st.code(sql, language="sql")

                        if not df.empty:
                            if chart_type != "table":
                                fig = render_chart(df, chart_type, prompt)
                                if fig:
                                    st.plotly_chart(fig, use_container_width=True)

                            with st.expander(f"📄 View Data ({len(df)} rows)"):
                                st.dataframe(df, use_container_width=True)

                        msg_id = str(datetime.now().timestamp())

                        # Save to history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "id": msg_id,
                            "question": prompt,
                            "sql": sql,
                            "summary": summary,
                            "chart_type": chart_type,
                            "df": df.to_dict(orient="records") if not df.empty else []
                        })

                        # Save graph button
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            if st.button("💾 Save Graph", key=f"save_new_{msg_id}"):
                                st.session_state.saved_graphs.append({
                                    "id": msg_id,
                                    "title": prompt[:50],
                                    "question": prompt,
                                    "sql": sql,
                                    "chart_type": chart_type,
                                    "df": df.to_dict(orient="records"),
                                    "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M")
                                })
                                st.success("✅ Graph saved!")

                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "id": str(datetime.now().timestamp()),
                            "question": prompt,
                            "sql": "",
                            "summary": f"Error: {str(e)}",
                            "chart_type": "table",
                            "df": []
                        })

# ─── PAGE: Saved Graphs ───────────────────────────────────────────
elif page == "📊 Saved Graphs":
    st.markdown("## 📊 Saved Graphs")

    if not st.session_state.saved_graphs:
        st.info("No saved graphs yet. Go to Chat, ask a question, and click 'Save Graph'!")
    else:
        st.markdown(f"**{len(st.session_state.saved_graphs)} saved graphs**")
        st.markdown("---")

        for i, graph in enumerate(st.session_state.saved_graphs):
            with st.expander(f"📊 {graph['title']} — {graph.get('saved_at', '')}"):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**Question:** {graph['question']}")
                with col2:
                    if st.button("🗑️ Delete", key=f"del_{i}"):
                        st.session_state.saved_graphs.pop(i)
                        st.rerun()

                # Show chart
                df = pd.DataFrame(graph["df"])
                if not df.empty:
                    if graph["chart_type"] != "table":
                        fig = render_chart(df, graph["chart_type"], graph["title"])
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)

                    # SQL
                    with st.expander("🔍 View SQL"):
                        st.code(graph["sql"], language="sql")

                    # Download with column selector
                    st.markdown("#### 📥 Download Data")
                    all_cols = list(df.columns)
                    selected_cols = st.multiselect(
                        "Select columns to download",
                        options=all_cols,
                        default=all_cols,
                        key=f"cols_{i}"
                    )
                    if selected_cols:
                        download_df = df[selected_cols]
                        csv = download_df.to_csv(index=False)
                        st.download_button(
                            label=f"⬇️ Download CSV ({len(selected_cols)} columns)",
                            data=csv,
                            file_name=f"{graph['title'][:30].replace(' ','_')}.csv",
                            mime="text/csv",
                            key=f"dl_{i}"
                        )
                        st.dataframe(download_df, use_container_width=True)

# ─── PAGE: Schema Explorer ────────────────────────────────────────
elif page == "🗄️ Schema Explorer":
    st.markdown("## 🗄️ Schema Explorer")
    st.markdown("Browse your database tables and columns.")

    try:
        df = run_query("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ('sales', 'customers')
            ORDER BY table_schema, table_name, ordinal_position
        """)

        for schema in ["sales", "customers"]:
            st.markdown(f"### 📁 Schema: `{schema}`")
            schema_df = df[df["table_schema"] == schema]
            for table in schema_df["table_name"].unique():
                table_df = schema_df[schema_df["table_name"] == table][["column_name", "data_type"]]
                with st.expander(f"📋 {schema}.{table} ({len(table_df)} columns)"):
                    st.dataframe(table_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Could not load schema: {e}")
