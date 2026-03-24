# DataWarehouse Chat — Streamlit App

Chat with your Supabase datawarehouse using plain English.

## Features
- 💬 Text-to-SQL chat interface
- 📊 Auto chart generation (bar, line, pie, scatter)
- 💾 Save graphs for later
- 📥 Download data with column selector
- 🗄️ Schema explorer

## Deploy to Streamlit Cloud

1. Push to GitHub
2. Go to share.streamlit.io
3. Connect your repo → select `app.py`
4. In **Advanced settings → Secrets**, add:
   ```
   DB_PASSWORD = "Amorodio.123"
   ```
5. Click Deploy!

## Local Development
```bash
pip install -r requirements.txt
streamlit run app.py
```
