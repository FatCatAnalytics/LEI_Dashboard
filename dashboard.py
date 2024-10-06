import streamlit as st
st.set_page_config(layout="wide")
st.markdown('<h1 style="font-size:24px;">FatCat Analytics Demo Corporate Hierarchy Dashboard</h1>', unsafe_allow_html=True)

home = st.Page("home.py", title="Information", icon=":material/home:")
upload_lei = st.Page("upload_lei.py", title="LEI Codes", icon=":material/update:")
upload_name = st.Page("upload_name.py", title="Corporate Names", icon=":material/update:")
view = st.Page("view.py", title="Data Overview", icon=":material/monitoring:")

# Define pages without "Home" section
pages = {
    "Home": [home],
    "Upload Data": [upload_lei, upload_name],
    "Analysis": [view],
}

# Navigation for other pages
pg = st.navigation(pages)
pg.run()