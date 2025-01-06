# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 16:41:47 2023

@author: User
"""

import streamlit as st
import pandas as pd
import numpy as np
import  snowflake.connector
import re
import base64
from st_pages import show_pages_from_config
import json
from PIL import Image

show_pages_from_config()

st.set_page_config(page_title="TAGS", layout="wide", page_icon='snowflakelogo.png')

header_style = '''
    <style>
        thead, tbody, tfoot, tr, td, th{
            background-color:#e9e9f2;
            color:#000;
        }
        
        section[data-testid="stSidebar"] div.stButton button {
          background-color: #049dbf;
          width: 300px;
          color: #fff;
        }
        .css-cgx0ld {
          background-color: #049dbf;
          color: #fff;
          font-size: 20px;
          float: right;
        }
        div.stButton button {
          background-color: #f7f7f7;
          color: #29b5e8;
          margin-top: 25px;
        }
        div[data-baseweb="select"] > div {
          background-color: #f7f7f7;
          color: #000;
        }
         
        .css-1b1e6xd {
          color: #000;
          font-weight: bold;
        }
        [data-testid='stSidebarNav'] > ul {
            min-height: 38vh;
        } 
        [data-testid="stSidebar"] {
                    background-image: url('https://pbsinfosystems.com/applogo.png');
                    background-repeat: no-repeat;
                    padding-top: 30px;
                    background-position: 20px 20px;
                }
    
    </style>
'''
st.markdown(header_style, unsafe_allow_html=True)


# Load credentials from app_config.json
def load_credentials():
    with open('app_config.json') as f:
        data = json.load(f)
        return data["snowflake"]

@st.cache_resource
def init_connection():
    # Use credentials from the JSON file
    creds = load_credentials()
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )

con = init_connection()
cur = con.cursor()

# image = Image.open('applogo.png')
# st.image(image)
 

with st.sidebar: 
    sql = "select distinct database_name from SNOWFLAKE.account_usage.databases"    
    cur.execute(sql)    
    results = cur.fetchall()    
    dblist = []
    
    for row in results:
        qryres = str(row[0])
        dblist.append(qryres)         
    
    default_ix = dblist.index('SNOWFLAKE')   
    

    def db_changed():
        st.session_state.name = st.session_state.name
        
    if 'name' in st.session_state:
        ##st.write(st.session_state['name'])
        default_ix = dblist.index(st.session_state['name'])   
    
        
    option = st.selectbox(
                'Select Database',
                options=dblist,
                index=default_ix,
                key='name',
                on_change = db_changed
            )
    ##st.write('You selected:', option)    
    
    db = st.session_state.name
    ##st.write(db)
    
    schsql = 'select SCHEMA_NAME from ' + db + '.information_Schema.schemata;'
    cur.execute(schsql)
    schresults = cur.fetchall()
    schemalist = []
    
    for row in schresults:
        schqryres = str(row[0])
        schemalist.append(schqryres)
        
    
    def schema_changed():
        st.session_state.schname = st.session_state.schname
        
        
    schemaoption = st.selectbox(
                        'Select Schema',
                        options=schemalist,
                        key='schname',
                        on_change = schema_changed
                        )
    
    schema = st.session_state.schname
    ##st.write(schema)
    

 



# Assuming you have a function from managetag.py
def managetag_tab():
    # Include the code from managetag.py here
    results7 = "" 
    
    with st.container():
        col1, col2, col3 = st.columns(3)
        
        with col1:
                
            sql1 = "SELECT distinct tag_name FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS WHERE TAG_DATABASE='" + db + "' AND TAG_SCHEMA='" + schema + "' ORDER BY TAG_NAME"    
            cur.execute(sql1)    
            results1 = cur.fetchall()    
            taglist = []
            
            for row in results1:
                qryres1 = str(row[0])            
                taglist.append(qryres1)
                
                        
            def tag_changed():
                st.session_state.tagname1 = st.session_state.tagname1             
            
            tagoption = st.selectbox(
                        'Select Tag',
                        options=taglist,                     
                        key='tagname1',
                        on_change = tag_changed
                    )
            
            tag = st.session_state.tagname1
            ##st.write(tag)
            
        with col2:
            st.text("")
            
        with col3:
            def update():
                st.session_state.name = st.session_state.name
                st.session_state.schname = st.session_state.schname            
                st.session_state.tagname1 = st.session_state.tagname1           
                
            
            viewobjectsbtn = st.button(":eye: View Objects", on_click=update, use_container_width=True)
            
            if viewobjectsbtn:
                if tagoption:
                    sql7 = "select tag_name AS "' "TAG NAME" '", tag_value AS "' "TAG VALUE" '", domain AS "' "OBJECT TYPE" '", object_name AS "' "OBJECT NAME" '",column_name AS "' "COLUMN NAME" '" from snowflake.account_usage.tag_references where tag_name="" '"+ tag +"' "" and TAG_DATABASE="" '"+ db +"' "";"
                    #st.write(sql7)
                    cur.execute(sql7)
                    results7 = cur.fetchall() 
                    #st.write(results7)
                    
    st.text("")
    st.text("")    
                
    with st.container():    
        
        if results7 != "":
            @st.cache_data(ttl=60)
            def defaultonload():
                df = pd.DataFrame(results7, columns=[desc[0] for desc in cur.description])            
                return df
            
            qryres7 = defaultonload()
            st.dataframe(qryres7, hide_index=True, use_container_width=True)
    pass

# Function containing the code from tag.py
def tag_tab():
    # The code you provided from tag.py goes here
    # For brevity, I'm not repeating the entire code here 
     
    output = "" 

    with st.container():
        col1, col2, col3 = st.columns(3)
        
        with col1:
                
            sql1 = "SELECT distinct tag_name FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS WHERE TAG_DATABASE='" + db + "' AND TAG_SCHEMA='" + schema + "' ORDER BY TAG_NAME"    
            cur.execute(sql1)    
            results1 = cur.fetchall()    
            taglist = []
            
            for row in results1:
                qryres1 = str(row[0])            
                taglist.append(qryres1)
                
                        
            def tag_changed():
                st.session_state.tagname = st.session_state.tagname
                
            
            tagoption = st.selectbox(
                        'Select Tag',
                        options=taglist,                     
                        key='tagname',
                        on_change = tag_changed
                    )
            
            tag = st.session_state.tagname
            ##st.write(tag)
            
        
        with col2:
            
            if tagoption:
            
                sql2 = "select system$get_tag_allowed_values('" + db + "." + schema + "." + tag + "');"
                cur.execute(sql2)
                results2 = cur.fetchall()        
                
                for row in results2:
                    qryres2 = str(row[0])   
                    #st.write(qryres2)
                    res = re.findall(r'\w+', qryres2)
                    #st.write(res)                
                        
                def tagvalue_changed():
                    st.session_state.tagvalue = st.session_state.tagvalue
                    
                
                tagvalueoption = st.selectbox(
                            'Select TagValue',
                            options=res,                     
                            key='tagvalue',
                            on_change = tagvalue_changed
                            )
                
                tagvalue = st.session_state.tagvalue
        
        with col3:               
            
            def update():
                st.session_state.name = st.session_state.name
                st.session_state.schname = st.session_state.schname
                st.session_state.tagname = st.session_state.tagname
                #st.session_state.options = st.session_state.options
                st.session_state.tagvalue = st.session_state.tagvalue
                
            
            applytagbtn = st.button(":heavy_plus_sign: Apply Tag", on_click=update, use_container_width=True)
            
            if applytagbtn:            
                        
                for table in st.session_state.tbllist:
                    st.write('Table Selected: ', table, db, schema, tag, tagvalue)                      
                    cur.execute("CALL APPLYTAG('"+ db +"','"+ schema +"','"+ table +"','"+ tag +"','"+ tagvalue +"')")
                    
                    output = cur.fetchone()
                    #st.write(output[0])                                                  

    with st.container():    
        
        if output != "":
        
            st.markdown("<div style='text-align: center;font-size:30px;font-weight: bold;color: green'>"+ str(output[0]) +"</div>",
                    unsafe_allow_html=True)       
        
        #@st.cache_data()
        def defaultonload(db):
            sql6 = 'select TABLE_NAME as "TABLE NAME" from ' + db + '.information_Schema.tables where table_type='" 'BASE TABLE' "';'
            #sql6 = 'select TABLE_NAME as "TABLE NAME" from ' + db + '.information_Schema.tables;'            
            cur.execute(sql6)            
            results6 = cur.fetchall()             
            df = pd.DataFrame(results6, columns=[desc[0] for desc in cur.description])            
            ##st.table(df)
            ##return df 
            
            if df.empty:
                html_string = "<h4 style='color:#000;text-align: center;'>No base tables available.</h4>"
                st.markdown(html_string, unsafe_allow_html=True)            
                return 'None'
                
            else:
                @st.cache_data(ttl=60)
                def gettagapplied():
                    alltables = df["TABLE NAME"]
                    #st.write(alltables)
                    
                    qryresults8 = []
                    for tbl in alltables:                
                        tbl = tbl.upper()                               
                        qryres8 = ''
                        try:                  
                            sql8 = ("SELECT TAG_NAME AS "' "Tag Applied" '" FROM TABLE(" + db + ".INFORMATION_SCHEMA.TAG_REFERENCES("" '"+ tbl +"' "", '" 'table' "') );")
                            #st.write(sql8)
                            cur.execute(sql8)
                            results8 = cur.fetchall()
                            #st.write(results8)
                            
                            if len(results8) == 0:
                                qryresult = "-"
                                qryresults8.append(qryresult)
                            else:
                                for roww in results8:
                                    qryres8 += str(roww[0]) + ','  
                                #st.write(qryres8)
                                
                                qryresult=qryres8.rstrip(',')
                                qryresults8.append(qryresult)     
                                
                        except:
                            qryresult = "-"
                            qryresults8.append(qryresult)
                            pass
                    
                    #st.write(qryresults8)                          
                    df1 = pd.DataFrame(qryresults8, columns=['TAG APPLIED'])
                    #st.write(df1)
                    return df1
                
                tagapplied = gettagapplied()
                    
                df = df.join(tagapplied)
                    
                df_with_selections = df.copy()
                df_with_selections.insert(0, "SELECT", False)        
                        
                edited_df = st.data_editor(
                    df_with_selections,
                    hide_index=True,
                    column_config={"SELECT": st.column_config.CheckboxColumn(required=True)},
                    disabled=df.columns,
                    use_container_width=True
                )
                selected_indices = list(np.where(edited_df.SELECT)[0])
                selected_rows = df[edited_df.SELECT]
                #st.write(selected_rows)
                selected_table = df[edited_df.SELECT].values
                
                selected_table = selected_table.tolist()
                #st.write(selected_table)
                st.session_state.tbllist = []
                
                for value in selected_table:
                    #st.write(value[0])
                    selectedtbl = value[0]
                    st.session_state.tbllist.append(selectedtbl)
                    
                #st.write(st.session_state.tbllist)           
                
                
                return {"selected_rows_indices": selected_indices, "selected_rows": selected_rows}     
        
                
        qryres6 = defaultonload(db)
        
        # if qryres6 != 'None':   
        #     st.write("Your selection:")
        #     st.write(qryres6)
    

# Main function to create tabs
def main():
    tab1, tab2 = st.tabs(["📑 Manage Tag", "🏷️ Apply Tag"])

    with tab1: 
        st.markdown(" ### :card_file_box: **Manage Tags**")
        managetag_tab()

    with tab2:
        st.markdown(" ### :card_file_box: **Table**")
        tag_tab()



if __name__ == "__main__":
    main()