import json
import streamlit as st
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval
from io import StringIO
from datetime import datetime
import modules.graphs as graphs
import modules.formats as formats
import modules.charts as charts
import modules.utils as utils
import uuid
from snowflake.snowpark import FileOperation

st.set_page_config(layout="wide")
st.title("Document Uploader")
st.caption("Upload documents for analysis")
css = '''
<style>
    [data-testid='stFileUploader'] {
        width: max-content;
    }
    [data-testid='stFileUploader'] section {
        padding: 0;
        float: left;
    }
    [data-testid='stFileUploader'] section > input + div {
        display: none;
    }
    [data-testid='stFileUploader'] section + div {
        float: right;
        padding-top: 0;
    }

</style>
'''

st.markdown(css, unsafe_allow_html=True)

def initializeSessionState():
    if 'createNewBatchButtonClicked' not in st.session_state:
        st.session_state.createNewBatchButtonClicked = False
    if 'user_id' not in st.session_state:
        st.session_state.user_id = "-1"
    if 'user_name' not in st.session_state:
        st.session_state.user_name = ""
    if 'is_admin' not in st.session_state:
        st.session_state.is_admin = ""
    if 'organization_id' not in st.session_state:
        st.session_state.organization_id = "-1"
    if 'organization_name' not in st.session_state:
        st.session_state.organization_name = ""
    if 'current_message' not in st.session_state:
        st.session_state.current_message = ""

def destroySessionState(_session):
    st.session_state.createNewBatchButtonClicked = False
    st.session_state.user_id = "-1"
    st.session_state.user_name = ""
    st.session_state.is_admin = "N"
    st.session_state.organization_id = "-1"
    st.session_state.organization_name = ""
    st.session_state.current_message = ""
    
    _session.close()
    _session = None    
    streamlit_js_eval(js_expressions="parent.window.location.reload()")

def putFileInStage(_session, stagePath, thisFile):
    
    bytes_data = thisFile.read()
    filename = thisFile.name
    filetype = thisFile.type
    filesize = thisFile.size
    #st.write("filename: " + filename)

    #stageQuery = """
    #    PUT file://C:\\testdelete\\abc.pdf @DOC_AI_DB.DOC_AI_SCHEMA.DOC_AI_STAGE overwrite=true auto_compress=false;
    #"""
    #st.write(stageQuery)
    #utils.sqlQuery(_session, stageQuery)
    
    stage_location = '@DOC_AI_STAGE/' + stagePath + '/' + filename
    #st.write(stage_location)    
    FileOperation(_session).put_stream(input_stream=thisFile, stage_location=stage_location, auto_compress=False)


def createNewBatchDetail(_session, batch_header_id, thisFile):
    #bytes_data = thisFile.read()
    filename = thisFile.name
    filetype = thisFile.type
    filesize = thisFile.size
    
    #st.write(thisFile)
    
    query = """
        INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_DETAIL(BATCH_HEADER_ID, FILE_NAME, FILE_TYPE, FILE_SIZE, BATCH_DETAIL_STATUS_CODE, APP_USER_ID_CREATED_BY, APP_USER_ID_MODIFIED_BY, CREATED_BY, MODIFIED_BY)
        VALUES('""" + str(batch_header_id) + """', '""" + str(filename) + """', '""" + str(filetype) + """', """ + str(filesize) + """, 'U', '""" + str(st.session_state.user_id) + """', '""" + str(st.session_state.user_id) + """', CURRENT_USER(), CURRENT_USER())
        """
    #st.write(query)
    #st.write(bytes_data)
    utils.sqlQuery(_session, query)

def createNewBatch(_session):
    st.session_state.createNewBatchButtonClicked = True
 
    numberfiles = len(uploaded_files)
    if  numberfiles > 0:
        #st.write("Select Model: " + selected_batch_header_model)
        new_batch_header_id = uuid.uuid4()
        new_batch_name =  str(datetime.today().strftime('%Y%m%d_%H%M%S_%f')) + "_BATCH"
        new_batch_path = str(datetime.today().strftime('%Y%m%d')) + "/" + str(st.session_state.organization_id) + "/" + new_batch_name
                
        query = """
            INSERT INTO DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER(ID, ORGANIZATION_ID, BATCH_NAME, BATCH_PATH, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_MODEL_CODE, APP_USER_ID_CREATED_BY, APP_USER_ID_MODIFIED_BY, CREATED_BY, MODIFIED_BY)
            VALUES('""" + str(new_batch_header_id) + """', '""" + str(st.session_state.organization_id) + """', '""" + new_batch_name + """', '""" + new_batch_path + """', 'U', '""" + selected_batch_header_model + """', '""" + str(st.session_state.user_id) + """', '""" + str(st.session_state.user_id) + """', CURRENT_USER(), CURRENT_USER())
            """
        #st.write(query)
        utils.sqlQuery(_session, query)
        
        for uploaded_file in uploaded_files:
            createNewBatchDetail(_session, new_batch_header_id, uploaded_file)
            #st.write("New Batch Name: " + new_batch_name)
            putFileInStage(_session, new_batch_path, uploaded_file)
            
            
initializeSessionState()

with st.sidebar:
    #session = (utils.getLocalSession()
    #    if utils.isLocal()
    #    else utils.getRemoteSession())
    session = utils.getLocalSession()
    
    df_user = None
    df_orig = None
    username = None
    organization_name = None
    organization_id = None
    if session is not None:
        username = st.text_input("User Name:")
        
    hasUsername = session is not None and username is not None and len(username) > 0
    if hasUsername:
        df_user = utils.getDataFrame(session, f"SELECT USER_ID, USER_NAME, IS_ADMIN, ORGANIZATION_ID, ORGANIZATION_NAME FROM DOC_AI_DB.SECURITY_SCHEMA.USERS_ORGANIZATIONS_VIEW WHERE USER_NAME='{username}'")
                
        if df_user.shape[0] > 0:
            st.session_state.user_id = df_user.iloc[0, 0]
            st.session_state.user_name = df_user.iloc[0, 1]
            st.session_state.is_admin = df_user.iloc[0, 2]
            st.session_state.organization_id = df_user.iloc[0, 3]
            st.session_state.organization_name = df_user.iloc[0, 4]
            
            
            st.write("User Name: " + st.session_state.user_name)
            st.write("Organization Name: " + st.session_state.organization_name)
            st.write("Is Admin: " + st.session_state.is_admin)
            #st.write("User ID: " + st.session_state.user_id)
            #st.write("Organization ID: " + st.session_state.organization_id)
            
            st.button("Logout", on_click=destroySessionState, args=(session,), use_container_width=False)
        else:
            st.write("Organization missing")
    

if st.session_state.organization_id != "-1" and st.session_state.organization_id is not None:
        
            
    if session is not None:
        
        if st.session_state.is_admin == 'Y':
            tabAdmin, tabCreateNewBatch, tabUnprocessedBatches, tabProcessedBatches = st.tabs(
                ["Administration", "Create New Batch", "Unprocessed Batches", "Processed Batches"])
            
            with tabAdmin:
                st.session_state.current_message = ""
                st.dataframe(df_user, use_container_width=True)
            
        else:    
            tabCreateNewBatch, tabUnprocessedBatches, tabProcessedBatches = st.tabs(
                ["Create New Batch", "Unprocessed Batches", "Processed Batches"])
        
                
        with tabCreateNewBatch:
            st.session_state.current_message = ""
            #st.dataframe(df_user, use_container_width=True)
            
        
            # Create a dropdown list of Batch Model Types
            # query all BHMs
            df_bhm = utils.getDataFrame(session, f"SELECT BATCH_HEADER_MODEL_CODE, BATCH_HEADER_MODEL_LABEL FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_MODEL WHERE ACTIVE_FLAG='Y'")
            
            # make lists of codes and labels, then combine into a dictionary containing each record (code/label pair)
            df_bhm_codes = df_bhm['BATCH_HEADER_MODEL_CODE'].to_numpy().tolist();
            df_bhm_labels = df_bhm['BATCH_HEADER_MODEL_LABEL'].to_numpy().tolist();
            df_bhm_codes_labels_dict = dict(zip(df_bhm_codes, df_bhm_labels))
            
            #implement lambda function. code is the value, name is the label for each option
            selected_batch_header_model = st.selectbox("Select a Document Model", df_bhm_codes_labels_dict.keys(), format_func=lambda x:df_bhm_codes_labels_dict[x])
            
            uploaded_files = st.file_uploader("Upload PDF Files", type=["pdf"], accept_multiple_files=True)
            st.button("Create New Batch", on_click=createNewBatch, args=(session,), use_container_width=False)
            
            if st.session_state.createNewBatchButtonClicked:
            # The message and nested widget will remain on the page
                numfiles = len(uploaded_files)
                if  numfiles > 0:
                    st.session_state.current_message = "New Batch has " + str(numfiles) + " files"
                else:
                    st.session_state.current_message = "No files selected. Please upload at least one file"
                st.write(st.session_state.current_message)
                st.session_state.createNewBatchButtonClicked = False
                st.session_state.current_message = ""
            
        with tabUnprocessedBatches:
            st.session_state.current_message = ""
            st.session_state.createNewBatchButtonClicked = False
            
            query = """
                SELECT BATCH_NAME, ORGANIZATION_NAME, BATCH_COUNT, BATCH_HEADER_MODEL_LABEL, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS
                FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW
                WHERE BATCH_HEADER_STATUS_CODE='U'
                AND ORGANIZATION_ID = '""" + str(st.session_state.organization_id) + """'
                ORDER BY BATCH_CREATED_ON DESC
                """
                
            df_records = utils.sqlQuery(session, query)
            print(df_records)
            st.write(df_records)
            
        with tabProcessedBatches:
            st.session_state.current_message = ""
            st.session_state.createNewBatchButtonClicked = False
            
            query = """
                SELECT BATCH_NAME, ORGANIZATION_NAME, BATCH_COUNT, BATCH_HEADER_MODEL_LABEL, BATCH_HEADER_STATUS_CODE, BATCH_HEADER_STATUS
                FROM DOC_AI_DB.STREAMLIT_SCHEMA.BATCH_HEADER_ORGANIZATION_VIEW
                WHERE BATCH_HEADER_STATUS_CODE='P'
                AND ORGANIZATION_ID = '""" + str(st.session_state.organization_id) + """'
                ORDER BY BATCH_CREATED_ON DESC
                """
                
            df_records = utils.sqlQuery(session, query)
            st.write(df_records)
else:
    st.write("Please login to create a batch")