from urllib2 import Request, urlopen, URLError
from xml.dom import minidom
from xml.etree import ElementTree as ET
import MySQLdb
import sys
import uuid
from datetime import datetime,timedelta

def getDbConnection(hostname,username,password):
    db_con = MySQLdb.connect(host=hostname,user=username,passwd=password,charset='utf8',use_unicode=True)
    return db_con
    
def setupDB(db_con):
    db_cursor=db_con.cursor()    
    db_cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'PubMedRepository'");
    db_stat=db_cursor.fetchone()

     
    if (not db_stat):
        start_date="2014-08-03"
        for mysql_stmt in open('setup_db.sql'):
            if mysql_stmt.strip():
                db_cursor.execute(mysql_stmt.strip())
        db_cursor.execute("insert into currentdate values (%s);",start_date)
	db_cursor.execute("commit")
	      
    db_cursor.close()
    

def getLastId(db_con):   
    db_cursor=db_con.cursor() 
    db_cursor.execute("SELECT LAST_INSERT_ID()")
    last_id=db_cursor.fetchone()
    db_cursor.close()
    return last_id[0]
    
def getLastInsertDate(db_con):
    db_cursor=db_con.cursor()
    db_cursor.execute("use PubMedRepository")
    db_cursor.execute("SELECT last_insert_date from currentdate");
    last_insert_date=db_cursor.fetchone()
    db_cursor.close()
    return last_insert_date[0]

def getData(xml, element):
    data_xml = xml.getElementsByTagName(element)
    if len(data_xml) > 0 and hasattr(data_xml[0].firstChild, 'data'):
        return data_xml[0].firstChild.data
    else:
        return "none"

        
def getAuthor(contributers):
    author = ""
    for contributer in contributers:
        name_main_xml = contributer.getElementsByTagName('name')
        name=""
        if len(name_main_xml) > 0:
            name_xml = name_main_xml[0]
            name = getData(name_xml, 'surname') + " " + getData(name_xml, 'given-names')
                    #do insert author here
        author = name+","+author
    return author

def getDate(pub_main_date_xml):
    pub_date=""
    if len(pub_main_date_xml) > 1:
        pub_date_xml = pub_main_date_xml[1]
        pub_year = getData(pub_date_xml , 'year')
        pub_month = getData(pub_date_xml , 'month')
        pub_day = getData(pub_date_xml , 'day')
        pub_date = pub_month + "-" + pub_day +"-" +pub_year
    elif len(pub_main_date_xml) > 0:
        pub_date_xml = pub_main_date_xml[0]
        pub_date = getData(pub_date_xml , 'year')
    else:
        pub_date = "none"
    return pub_date

def childData(node):
    abstract = ""
    if hasattr(node.firstChild, 'data'):
        abstract = abstract + node.firstChild.data
    if hasattr(node,'data'):
        abstract = abstract + node.data
    if node.hasChildNodes():
        children = node.childNodes
        for child in children:
           abstract = abstract + childData(child) 
    return abstract

def getAbstract(abstract_main_xml):
    abstract = ""
    if len(abstract_main_xml) > 0:
        abstract_child = abstract_main_xml[0].childNodes
        for child in abstract_child:
            abstract = abstract + childData(child)
    return abstract

def getRefAuthor(name_xml):
    total_name=""
    for names in name_xml:
        surname_xml = names.getElementsByTagName('surname')
        given_name_xml = names.getElementsByTagName('given-names')
        name= ""
        if len(surname_xml) > 0: 
            if hasattr(surname_xml[0].firstChild, 'data'):
                name = surname_xml[0].firstChild.data 
        if len(given_name_xml) > 0:
            if hasattr(given_name_xml[0].firstChild, 'data'):
                name = name + given_name_xml[0].firstChild.data 
        total_name = name + "," + total_name
    return total_name

def dataFetcher(main_url,db_con):
    db_cursor=db_con.cursor()
    db_cursor.execute("use PubMedRepository")
    count =0
    resumption ="none"
    db_cursor.execute("start transaction")
    art_num=1
    while True:
        url =""
        if resumption is not "none":
            url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&resumptionToken=' + resumption
        else:
            url = main_url
        requestURL = Request(url)
        try:
            response = urlopen(requestURL)
            xmldoc = minidom.parseString(response.read())
            elements = xmldoc.getElementsByTagName('record')
            
            for record in elements:
                print "Retrieving article "+str(art_num)
                uid=uuid.uuid4()
                journalId = getData(record, 'journal-id')
                journalTitle = getData(record, 'journal-title')
                title = getData(record,'article-title')
                pub_main_date_xml=record.getElementsByTagName('pub-date')
                pub_date=getDate(pub_main_date_xml)
                pmid_xml=record.getElementsByTagName('article-id')
                pmid = "none"
                pmc_uid = "none"
                pmc = "none"
                accession = "none"
                publisher_id = "none"
                doi = "none"
                for ids in pmid_xml:
                    if ids.attributes['pub-id-type'].value == "accession":
                        accession = ids.firstChild.data
                    elif ids.attributes['pub-id-type'].value == "pmcid":
                        pmc = ids.firstChild.data
                    elif ids.attributes['pub-id-type'].value == "pmc-uid":
                        pmc_uid = ids.firstChild.data
                    elif ids.attributes['pub-id-type'].value == "pmid":
                        pmid = ids.firstChild.data
                    elif ids.attributes['pub-id-type'].value == "publisher-id":
                        publisher_id = ids.firstChild.data
                    elif ids.attributes['pub-id-type'].value == "doi":
                        doi = ids.firstChild.data
                #insert article here
                contributers = record.getElementsByTagName('contrib')
                author=getAuthor(contributers) 
            
                if pmc_uid != "none":
                    fullTextURL = "http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:"+pmc_uid+"&metadataPrefix=pmc"
                    fullTextRequestURL = Request(fullTextURL)
                    try:
                        fullTextresponse = urlopen(fullTextRequestURL)
                        fullTextxmldoc = minidom.parseString(fullTextresponse.read())
                        abstract_main_xml = fullTextxmldoc.getElementsByTagName('abstract')
                        abstract = getAbstract(abstract_main_xml)
                        db_cursor.execute("insert into article_meta values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(uid,accession,pmc,pmc_uid,publisher_id,pmid,doi,title,journalId,journalTitle,pub_date,abstract,author))
                        references = fullTextxmldoc.getElementsByTagName('ref')
                        ref_num=1
                        for reference in references:
                            print "Retrieving reference "+str(ref_num)
                            rid=uuid.uuid4()
                            name_xml = reference.getElementsByTagName('name')
                            total_name=getRefAuthor(name_xml)
                            ref_title = getData(reference,'article-title')
                            ref_id = getData(reference,'pub-id')
			    db_cursor.execute("insert into article_references values (%s,%s,%s,%s,%s,%s)",(rid,ref_id,uid,ref_title,total_name,pmid))
                            ref_num=ref_num+1
                    except URLError, e:
                        exception = exception + 1         
            	art_num=art_num+1          
            resumption = getData(xmldoc,'resumptionToken')
            
                #insert reference here 
        except URLError, e:
            print 'Got an error code:', e
        if resumption == "none":
            db_cursor.execute("commit")
            break;
    db_cursor.execute("update currentdate set last_insert_date=(%s)",datetime.utcnow().isoformat())
    db_cursor.execute("commit")
    db_cursor.close()

def main():
    hostname=sys.argv[1]
    username=sys.argv[2]
    password=sys.argv[3]
    
    db_con=getDbConnection(hostname,username,password)
    print 'type of db_con'+str(type(db_con))
    try:       
        setupDB(db_con)
        lastdate=getLastInsertDate(db_con)
        temp = lastdate.strftime("%Y-%m-%d")
        if(temp == '2014-08-03'):
            url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&from='+temp+'&metadataPrefix=pmc_fm&set=pmc-open'
            dataFetcher(url,db_con)
        else:
            lastdate +=timedelta(days=1)
            datestring = lastdate.strftime("%Y-%m-%d")
            url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&from='+datestring+'&metadataPrefix=pmc_fm&set=pmc-open'
            dataFetcher(url,db_con)
   
    finally:
        db_con.close()
    #other function calls
    

if __name__ == "__main__":
    main()  

#url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&from=2001-01-01&metadataPrefix=pmc_fm&set=pmc-open'
