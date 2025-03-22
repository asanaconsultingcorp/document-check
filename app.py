import json
import streamlit as st
import streamlit.components.v1 as components
from io import StringIO
import modules.graphs as graphs
import modules.formats as formats
import modules.charts as charts
import modules.utils as utils

st.set_page_config(layout="wide")
st.title("Document Uploader")
st.caption("Display your hierarchical data with charts and graphs.")


with st.sidebar:
    session = (utils.getLocalSession()
        if utils.isLocal()
        else utils.getRemoteSession())

    df_user = None
    df_orig = None
    username = None
    if session is not None:
        username = st.text_input("User Name:")
        
    hasUsername = session is not None and username is not None and len(username) > 0
    if hasUsername:
        df_user = utils.getDataFrame(session, f"select organization_name from DOC_AI_DB.SECURITY_SCHEMA.USERS_ORGANIZATIONS where user_name='{username}'")
        st.write("User Name: " + username)
        
        if df_user.shape[0] > 0:
            st.write("Organization: " + df_user.iloc[0][0])
        else:
            st.write("Organization missing")
    
