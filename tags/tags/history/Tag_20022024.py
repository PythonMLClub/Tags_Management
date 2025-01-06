# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 16:41:47 2023

@author: User
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
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
    sql = "show databases"    
    cur.execute(sql)    
    results = cur.fetchall()    
    dblist = []
    # st.write("db names")
    # st.write(results)
    for row in results:
        qryres = str(row[1])
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
                
            # sql1 = "SELECT distinct tag_name FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS WHERE TAG_DATABASE='" + db + "' AND TAG_SCHEMA='" + schema + "' ORDER BY TAG_NAME"
            # sql1 = "show tags" 
            sql1 = "SELECT distinct tag_name FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS WHERE TAG_DATABASE='" + db + "' AND TAG_SCHEMA='" + schema + "' ORDER BY TAG_NAME"   
            cur.execute(sql1)    
            results1 = cur.fetchall()    
            taglist = []
            # st.write("db name")
            # st.write(results1)
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
                # st.write(st.session_state.name)
                # st.write(st.session_state.schname)
                # st.write(st.session_state.tagname1)
                # tag = st.session_state.tagname1
                # st.write("function tag value selected : ")
                # st.write(tag)
                # return tag
            viewobjectsbtn = st.button(":eye: View Objects", on_click=update, use_container_width=True)
            
            if viewobjectsbtn:
                if tagoption:
                    #st.write("tag value selected : ")
                    #st.write(tag)
                    #sql7 = "select tag_name AS "' "TAG NAME" '", tag_value AS "' "TAG VALUE" '", domain AS "' "OBJECT TYPE" '", object_name AS "' "OBJECT NAME" '",column_name AS "' "COLUMN NAME" '" from snowflake.account_usage.tag_references where tag_name="" '"+ tag +"' "" and TAG_DATABASE="" '"+ db +"' "";"
                    sql7 = "select OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, TAG_NAME, TAG_VALUE, DOMAIN from snowflake.account_usage.tag_references where tag_name= "" '"+ tag +"' "" and OBJECT_DATABASE= "" '"+ db +"' "" and OBJECT_SCHEMA= "" '"+ schema +"' "" ;"
                    #st.write(sql7)
                    cur.execute(sql7)
                    results7 = cur.fetchall()

                    mgwithtagdf = pd.DataFrame(results7, columns=[desc[0] for desc in cur.description]) 
                    #st.write(mgwithtagdf)
                    alltag = mgwithtagdf["TAG_NAME"]
                    alldb = mgwithtagdf["OBJECT_DATABASE"]
                    alltable = mgwithtagdf["OBJECT_NAME"]
                    allschema = mgwithtagdf["OBJECT_SCHEMA"]
                    alldomain = mgwithtagdf["DOMAIN"]
                    # st.write(alltables)
                    # st.write(allcolumns)
                    qryresults8 = []
                    for taglist, dblist, taablelist, schemalist, domainlist in zip(alltag, alldb, alltable, allschema, alldomain):                
                        taglist = taglist.upper() 
                        dblist = dblist.upper()
                        taablelist = taablelist.upper()
                        schemalist = schemalist.upper()
                        domainlist = domainlist.upper()
                        qryres8 = ''
                        try:
                            if domainlist == "COLUMN":
                            #sql8 = ("SELECT TAG_NAME AS "' "Tag Applied" '" FROM TABLE(" + db + ".INFORMATION_SCHEMA.TAG_REFERENCES("" '"+ tbl +"."+ clm +"' "", '" 'column' "') );")
                                sql8 = ("select COLUMN_NAME from snowflake.account_usage.tag_references where OBJECT_DATABASE= "" '"+ db +"' "" and OBJECT_SCHEMA= "" '"+ schema +"' "" and object_name="" '"+ taablelist +"' "" and tag_name="" '"+ taglist +"' "" and domain ='COLUMN'")
                                #st.write(sql8)
                                cur.execute(sql8)
                                results8 = cur.fetchall()
                                # st.write("results8:")
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
                            else:
                                qryresult = "-"
                                qryresults8.append(qryresult)
                                
                                
                        except:
                            qryresult = "-"
                            qryresults8.append(qryresult)
                            pass
                    columndf1 = pd.DataFrame(qryresults8, columns=['COLUMN_NAME'])
                    #st.write(columndf1)
                    
    st.text("")
    st.text("")    
                
    with st.container():    
        
        if results7 != "":
            #@st.cache_data(ttl=60)
            # def defaultonload():
            #     manageresult = pd.DataFrame(results7, columns=[desc[0] for desc in cur.description])            
            #     return manageresult
            
            # manageout = defaultonload()
            # st.write(manageout)
            viewobjtagdf = mgwithtagdf.join(columndf1)
            #st.write(viewobjtagdf)
            viewobjtagdf = viewobjtagdf.drop_duplicates(subset=["OBJECT_DATABASE", "OBJECT_SCHEMA", "OBJECT_NAME", "TAG_NAME", "TAG_VALUE", "COLUMN_NAME"])
            st.dataframe(viewobjtagdf, hide_index=True, use_container_width=True)
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
            st.text("")
            # def update():
            #     st.session_state.name = st.session_state.name
            #     st.session_state.schname = st.session_state.schname
            #     st.session_state.tagname = st.session_state.tagname
            #     #st.session_state.options = st.session_state.options
            #     st.session_state.tagvalue = st.session_state.tagvalue
                
            
            # applytagbtn = st.button(":heavy_plus_sign: Apply Tag", on_click=update, use_container_width=True)
            
            # if applytagbtn:            
                        
            #     for table, column in st.session_state.tbllist:
            #         st.write('Table Selected: ', table)     
            #         #call TAG_Management_ApplyTag('trial_123','public','vw_sample','cost_center_na','finance','column','employee_phone')                 
            #         # cur.execute("CALL APPLYTAG('"+ db +"','"+ schema +"','"+ table +"','"+ tag +"','"+ tagvalue +"')")
            #         cur.execute("CALL AG_Management_ApplyTag('"+ db +"','"+ schema +"','"+ table +"','"+ tag +"','"+ tagvalue +"')")
                    
            #         output = cur.fetchone()
                    #st.write(output[0])                                                  
            #st.markdown(f"<div style='text-align: center;'>{output}</div>", unsafe_allow_html=True)


    with st.container(): 

        col1, col2, col3 = st.columns(3)
        
        with col1:
            def objtype_changed():
                st.session_state.objtype = st.session_state.objtype
            
            table2 = st.selectbox(
                'Select Type',
                options=('Table', 'View', 'Column'),
                key='objtype',
                on_change = objtype_changed)
            
            obj_type = st.session_state.objtype

            #st.write("object type :")
            #st.write(obj_type)

            
        with col2: 
            table3 = st.selectbox(
            'Name pattern',
            options=('Starts with', 'Ends with', 'Contains'),
            key='tblpatt')
            
            table_pattern = st.session_state.tblpatt
            #st.write("tablepattern :")
            #st.write(table_pattern)
             
        with col3:
            table4 = st.text_input('Type pattern : Eg cust_tbl', key='name1')
       
            pattern_value = st.session_state.name1

            #st.write("patternvalue :")
            #st.write(pattern_value)
            #st.write('pattern typed:', patternvalue)
    

    with st.container():    
        
        # if output != "":
        
        #     st.markdown("<div style='text-align: center;font-size:30px;font-weight: bold;color: green'>"+ str(output[0]) +"</div>",
        #             unsafe_allow_html=True)       
        # def dataframe_with_selections(searchtagdf):
        #     df_with_selections = searchtagdf.copy()
        #     df_with_selections.insert(0, "Select", False)

        #     # Get dataframe row-selections from user with st.data_editor
        #     edited_df = st.data_editor(
        #         df_with_selections,
        #         hide_index=True,
        #         column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        #         disabled=searchtagdf.columns,
        #     )

        #     # Filter the dataframe using the temporary column, then drop the column
        #     selected_rows = edited_df[edited_df.Select]
        #     return st.write(selected_rows)
        

        
        @st.cache_data(experimental_allow_widgets=True)
        def execute_query(db, schema, obj_type, table_pattern, pattern_value):
            if obj_type.lower() == 'table':
                table_type_condition = "table_type='BASE TABLE'"
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name FROM {db}.information_schema.tables WHERE table_schema='{schema}' AND {table_type_condition} AND {table_pattern}(LOWER(table_name), '{pattern_value}');"
                st.write("Executing query:", search_query)
                cur.execute(search_query)     
                #table_names = [row[0] for row in cur.fetchall()]       
                searchresults = cur.fetchall() 
                #searchdf = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])          
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description]) 
                #getalltableNames = pd.DataFrame({"TABLE_NAME": searchdf["TABLE_NAME"]})
                #st.write(getalltableNames)

            elif obj_type.lower() == 'view':
                table_type_condition = "table_type='VIEW'"
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name FROM {db}.information_schema.tables WHERE table_schema='{schema}' AND {table_type_condition} AND {table_pattern}(LOWER(table_name), '{pattern_value}');"
                st.write("Executing query:", search_query)
                cur.execute(search_query)     
                #table_names = [row[0] for row in cur.fetchall()]       
                searchresults = cur.fetchall() 
                #searchdf = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])          
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description]) 
                #getalltableNames = pd.DataFrame({"TABLE_NAME": searchdf["TABLE_NAME"]})
                #st.write(getalltableNames)
            elif obj_type.lower() == 'column':
                table_pattern = table_pattern.replace(' ', '')
                search_query = f"SELECT table_name,column_name FROM {db}.information_schema.columns WHERE table_schema='{schema}' AND {table_pattern}(LOWER(column_name), '{pattern_value}');"
                st.write("Executing query:", search_query)
                cur.execute(search_query)     
                #table_names = [row[0] for row in cur.fetchall()]       
                searchresults = cur.fetchall() 
                #searchdf = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description])          
                getalltableNames = pd.DataFrame(searchresults, columns=[desc[0] for desc in cur.description]) 
                #getalltableNames = pd.DataFrame({"TABLE_NAME": searchdf["TABLE_NAME"]})
                #st.write(getalltableNames)
            else:
                search_query = ""
            
        
            if getalltableNames.empty:
                html_string = "<h4 style='color:#000;text-align: center;'>No base tables available.</h4>"
                st.markdown(html_string, unsafe_allow_html=True)            
                return 'None'
                
            else:
                @st.cache_data(ttl=60)
                def gettagapplied(obj_type):
                    st.write("selected object type")  
                    selected_type = st.session_state.objtype
                    st.write(selected_type)
                    
                    if selected_type.lower() == 'table' or selected_type.lower() == 'view':
                        alltables = getalltableNames["TABLE_NAME"]
                        #st.write("DataFrame table")
                        qryresults8 = []
                        for tbl in alltables:                
                            tbl = tbl.upper() 
                            qryres8 = ''
                            try: 
                                sql8 = ("SELECT TAG_NAME AS "' "Tag Applied" '" FROM TABLE(" + db + ".INFORMATION_SCHEMA.TAG_REFERENCES("" '"+ tbl +"' "", '" + selected_type.lower() + "') );")
                                st.write(sql8)
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
                        df1 = pd.DataFrame(qryresults8, columns=['TAG APPLIED'])
                        #st.write(df1)
                        return df1
                    elif selected_type.lower() == 'column':
                        alltables = getalltableNames["TABLE_NAME"]
                        allcolumns = getalltableNames["COLUMN_NAME"]
                        # st.write(alltables)
                        # st.write(allcolumns)
                        qryresults8 = []
                        for tbl, clm in zip(alltables, allcolumns):                
                            tbl = tbl.upper() 
                            clm = clm.upper()
                            qryres8 = ''
                            try: 
                                sql8 = ("SELECT TAG_NAME AS "' "Tag Applied" '" FROM TABLE(" + db + ".INFORMATION_SCHEMA.TAG_REFERENCES("" '"+ tbl +"."+ clm +"' "", '" 'column' "') );")
                                st.write(sql8)
                                cur.execute(sql8)
                                results8 = cur.fetchall()
                                # st.write("results8:")
                                # st.write(results8)
                                
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
                        df1 = pd.DataFrame(qryresults8, columns=['TAG APPLIED'])
                        #st.write(df1)
                        return df1
                        st.write("DataFrame has TABLE_NAME, COLUMN_NAME")
                    
                
                
                tagapplied = gettagapplied(obj_type) 
                #st.write(tagapplied)    
                searchtagdf = getalltableNames.join(tagapplied)
                #st.write(searchtagdf) 

                # Call the function to add selection column and get the edited DataFrame
                #edited_df = dataframe_with_selections(searchtagdf)
                searchdf_with_selections = searchtagdf.copy()
                searchdf_with_selections.insert(0, "SELECT", False)        
                # @st.cache_data()
                # def edit_dataframe(searchdf_with_selections):
                #     return st.data_editor(
                #         searchdf_with_selections,
                #         hide_index=True,
                #         column_config={"SELECT": st.column_config.CheckboxColumn(required=True)},
                #         disabled=searchtagdf.columns,
                #         use_container_width=True
                #     )

                # Use the memoized function
                #edited_df = edit_dataframe(searchdf_with_selections) 
                edited_df = st.data_editor(
                    searchdf_with_selections,
                    hide_index=True,
                    column_config={"SELECT": st.column_config.CheckboxColumn(required=True)},
                    disabled=searchtagdf.columns,
                    use_container_width=True
                )
                selected_indices = list(np.where(edited_df.SELECT)[0])
                # Initialize lists to store table names and column names
                selected_table_names = []
                selected_column_names = []
                
                # Iterate over selected indices to extract table names and column names
                for index in selected_indices:
                    if 'TABLE_NAME' in edited_df.columns and 'COLUMN_NAME' in edited_df.columns:
                        row = edited_df.iloc[index]
                        selected_table_name = row['TABLE_NAME']
                        selected_column_name = row['COLUMN_NAME']
                    else:
                        row = edited_df.iloc[index]
                        selected_table_name = row['TABLE_NAME']
                        # In this case, we assume that there are no separate column names, so we set column names to None
                        selected_column_name = ''
                    selected_table_names.append(selected_table_name)
                    selected_column_names.append(selected_column_name)
                selected_tables_and_columns = list(zip(selected_table_names, selected_column_names))   
                st.session_state.tbllist = selected_tables_and_columns  
                #st.write(st.session_state.tbllist)           
                
                
                return {"selected_rows_indices": selected_indices}   
            pass
        if pattern_value != '':
            tagvalue = st.session_state.tagvalue
            tag = st.session_state.tagname
            qryres6 = execute_query(db, schema, obj_type, table_pattern.lower(), pattern_value.lower())
            if qryres6 != 'None':
                selected_tables_and_columns = st.session_state.tbllist   
                st.write(f"You have selected {tag} and tag value is {tagvalue} and selected table {st.session_state.tbllist}")
                #st.write(qryres6)

    # ss_tagname =''
    # ss_dbname=''
    # ss_pattern_value=''
    # if st.button("Search"):
    #     ss_dbname = st.session_state.name
    #     ss_schname = st.session_state.schname
    #     ss_tagname = st.session_state.tagname
    #     ss_obj_type = st.session_state.objtype
    #     ss_table_pattern = st.session_state.tblpatt
    #     ss_pattern_value = st.session_state.name1
    
    # # if dbname and schname and tagname and obj_type and table_pattern != '':
    # if ss_pattern_value !='':
    #     st.write(f"dbname: {ss_dbname}, schname: {ss_schname}, tagname: {ss_tagname}, obj_type: {ss_obj_type}, table_pattern: {ss_table_pattern}")
    #     execute_query(ss_dbname, ss_schname, ss_obj_type, ss_table_pattern.lower(), ss_pattern_value.lower())
    # else:
    #     st.write("Please fill in all required fields.")
    

    # if st.button("Search"): 
    #     tagvalue = st.session_state.tagvalue
    #     tag = st.session_state.tagname
    #     st.write(f"You have selected {tag} and tag value is {tagvalue}")
    #     #execute_query(db, schema, obj_type, table_pattern.lower(), pattern_value.lower())
    #     qryres6 = execute_query(db, schema, obj_type, table_pattern.lower(), pattern_value.lower())
    #     if qryres6 != 'None':
    #         selected_tables_and_columns = st.session_state.tbllist   
    #         st.write(f"You have selected {tag} and tag value is {tagvalue} and selected table {st.session_state.tbllist}")
    #         st.write(qryres6)



    with st.container():
        col1, col2, col3 = st.columns(3)
        with col1:
            st.text("") 
        with col2:
            st.text("")
        with col3:   
            st.text("Please enter a value for pattern to search and press Enter key")
            def update():
                st.session_state.name = st.session_state.name
                st.session_state.schname = st.session_state.schname
                st.session_state.tagname = st.session_state.tagname
                #st.session_state.options = st.session_state.options
                st.session_state.tagvalue = st.session_state.tagvalue

            applytagbtn = st.button(":heavy_plus_sign: Apply Tag", on_click=update, use_container_width=True)
            
            if applytagbtn:            
                    
                for table, columnvalues in st.session_state.tbllist:
                    #st.write('Table Selected: ', table)     
                    #call TAG_Management_ApplyTag('trial_123','public','vw_sample','cost_center_na','finance','column','employee_phone')                 
                    # cur.execute("CALL APPLYTAG('"+ db +"','"+ schema +"','"+ table +"','"+ tag +"','"+ tagvalue +"')")
                    cur.execute("CALL TAG_Management_ApplyTag('"+ db +"','"+ schema +"','"+ table +"','"+ tag +"','"+ tagvalue +"','"+ obj_type +"','"+ columnvalues +"')")
                    
                    output = cur.fetchone()
                #st.write(output[0])                                                  
        # st.markdown(f"<div style='text-align: center;'>{output}</div>", unsafe_allow_html=True)
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