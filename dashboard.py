import streamlit as st
st.set_page_config(layout="wide")
st.markdown('<h1 style="font-size:24px;">FatCat Analytics Demo Corporate Hierarchy Dashboard</h1>', unsafe_allow_html=True)
st.write('This dashboard is a prototype for the FatCat Analytics Corporate Hierarchy Dashboard.'
         ' It allows users to seach for Legal Entity Identifier (LEI) data or corporate names, fetch the hierarchy data,'
         ' and view the data in an interactive graph. The dashboard is divided into three main sections: '
            '1. **Home**: Information about the dashboard. '
            '2. **Upload Data**: Upload LEI codes or corporate names to fetch the hierarchy data. '
            '3. **Analysis**: View the hierarchy data and search for specific entities. '
            'Use the sidebar to navigate between sections.')

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