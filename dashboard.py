import streamlit as st
st.set_page_config(layout="wide")
st.markdown('<h1 style="font-size:24px;">Coalition Greenwich Corporate Hierarchy Dashboard</h1>', unsafe_allow_html=True)
st.write('This dashboard is a prototype for the Coalition Greenwich Corporate Hierarchy Dashboard.')


pages = {
    "Home": [st.Page("Home.py", title="Home")],
    "Upload Data": [st.Page("Upload_LEI.py", title="Search Using LEI Codes"),
             st.Page("Upload_Name.py", title="Seach Using Corporate Names"),],
    "Analysis": [st.Page("View.py", title="Data Overview")],

}

pg = st.navigation(pages)
pg.run()
