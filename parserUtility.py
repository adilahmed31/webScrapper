from databaseUtility import *
from utility import *
from datetime import datetime
import time
import json
import requests
from bs4 import BeautifulSoup
import base64
import math
import re
import sqlite3 as sql
import pandas as pd

def tencent(db,q):
    print("Crawling Tencent")
    numberOfTerms = 0
    while(q.empty() != True):
        time.sleep(1)
        word = q.get()
        print("Starting " + word + " with queue length " + str(q.qsize()))
        with open("crawled_tencent.txt") as file:
            if word in file:
                continue
        appIDList = ""
        #For tencent, the pns parameter is incremented by 10 in each call to return all requests for a particular query.
        #the parameter value is base64 encoded before sending the request
        #The loop terminates when no further results are obtained (try/except statement)
        for count in range(0,1000,10):
            count_bytes = str(count).encode('ascii')
            payload = {'kw':word,'pns':base64.b64encode(count_bytes).decode('ascii'),'sid':''}
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            r = requests.get('https://android.myapp.com/myapp/searchAjax.htm', params=payload, headers=headers)
            response = r.json()
            currentTime = datetime.now()
            # Create appDetailsTable in DB
            appDetailsTable = getTable(db, 'AppDetails')
            first = 0
            try:
                for item in response["obj"]["items"]:
                    appID = item["pkgName"]
                    fileSize = item["appDetail"]["fileSize"]
                    apkHash = item["appDetail"]["apkMd5"]
                    downloadURL = item["appDetail"]["apkUrl"]
                    title = item["appDetail"]["appName"]
                    desc = item["appDetail"]["description"]
                    developerName = item["appDetail"]["authorName"]
                    version = item["appDetail"]["versionName"]
                    category = version = item["appDetail"]["categoryName"]
                    averageRating = item["appDetail"]["averageRating"]
                    imageLink = item["appDetail"]["iconUrl"]
                    #savedetailsinDB
                    insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, appName=title, downloadURL=downloadURL,
                        desc=desc,file_size=fileSize,authorName=developerName,rating=averageRating,category=category,
                            filehash=apkHash, imageSource=imageLink, developerName=developerName, 
                                websiteName='android.myapp.com', createdAt=currentTime,version=version,other=str(item)))
            except TypeError:
                print("All results returned for query!")
                break
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
        # Create appIdTable & suggestionTable in DB
        appIdTable = getTable(db, 'AppId')    

        # Create entries for tables
        currentTime = datetime.now()

        # Enter into appIdTable
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'android.myapp.com', createdAt = currentTime))
        with open("crawled_tencent.txt","a") as file:
            file.write(word + "\n")

def store360(db,q):
    print("Crawling 360 Mobile Assistant Store")
    # Create appDetailsTable in DB
    appDetailsTable = getTable(db, 'AppDetails')
    while(q.empty() != True):
        time.sleep(1)
        word = q.get()
        # Time
        currentTime = datetime.now()
        appIDList = ""
        first = 0
        payload = {'kw':word}
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        r = requests.get('http://zhushou.360.cn/search/index', params=payload, headers=headers)
        soup = BeautifulSoup(r.text,'html.parser')
        for number in soup.find_all('span'):
            if number.parent.name == 'h2' and number.contents[0].isnumeric() == True:
                numberOfResults = number.contents[0]
        numberOfPages = math.ceil(int(numberOfResults)/15.0)
        for page in range(numberOfPages):
            payload = {'kw':word,'page':page}
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            r = requests.get('http://zhushou.360.cn/search/index', params=payload, headers=headers)
            soup = BeautifulSoup(r.text,'html.parser')
            for app in soup.find_all('a'):
                try:
                    if app.parent.name == 'h3':
                        #Construct URL to fetch app details. id parameter is a generic UUID, not to be confused with app ID
                        detailsUrl = "http://zhushou.360.cn" + str(app["href"])
                        print(detailsUrl)
                        id_start_pos = detailsUrl.rfind("/")
                        id_end_pos = detailsUrl.rfind("?")
                        id = detailsUrl[id_start_pos+1:id_end_pos]
                        r = requests.get(detailsUrl)
                        soup = BeautifulSoup(r.text, 'html.parser')

                        #Extract details from details variable within js function on the page
                        pattern = re.compile(r'var\sdetail\s=\s\(function',re.MULTILINE | re.DOTALL)
                        js_details = soup.find("script",text=pattern)
                        js_details = js_details.string
                        match = re.search(r"\{.*(\{.*?\}).*",js_details,re.MULTILINE | re.DOTALL)
                        details = match.group(1)
                        #Convert to valid JSON format
                        details = details.replace('\'','\"')
                        #Convert to JSON object
                        details = json.loads(details)
                        appID = details["pname"]
                        print(appID)
                        md5_hash = details["filemd5"]

                        #Extract remaining details from HTML content
                        appName_end_pos = str(soup.find(id='app-name').contents[0]).rfind("</span")
                        appName_start_pos = str(soup.find(id='app-name').contents[0]).rfind(">",0,appName_end_pos)
                        appName = str(soup.find(id='app-name').contents[0])[appName_start_pos+1:appName_end_pos]
                        for element in soup.find_all('span',class_ = "s-1 js-votepanel"):
                            rating = element.contents[0]
                        downloadUrl = "https://app.api.sj.360.cn/url/download/id/" + id + "/from/web_detail"
                        baseInfo = soup.find_all("div",class_ = "base-info")
                        info = BeautifulSoup(str(baseInfo), 'html.parser')
                        metadata = list()
                        for element in info.find_all('td'):
                            metadata.append(element.contents[1])
                        authorName = metadata[0]
                        publishDate = metadata[1]
                        version = metadata[2]
                        osVersion = metadata[3]

                        #Fetch Description
                        #Fetch Description
                        desc_html = soup.find("div", class_ = "breif")
                        desc_text = desc_html.get_text()
                        desc = re.sub(r"\n+","\n",desc_text)

                        app_data = {"appID":appID, "hash": md5_hash, "App_name":appName, "rating":rating, "desc":desc,
                                "authorName":authorName, "publishDate" : publishDate, "version":version, "downloadURL":downloadUrl}
                        insertIntoAppDetailsTable(appDetailsTable, dict(appID=app_data["appID"], desc = app_data["desc"], appName=app_data["App_name"], rating=app_data["rating"],downloadURL=app_data["downloadURL"],
                            version=app_data["version"],authorName=app_data["authorName"],publishDate=app_data["publishDate"], websiteName='zhushou.360.cn', createdAt=currentTime, filehash = app_data["hash"]))
                except AttributeError as e:
                    continue


def baidu(db,q):
    print("Crawling Baidu App Store")
    # Create appDetailsTable in DB
    appDetailsTable = getTable(db, 'AppDetails')
    while(q.empty() != True):
        time.sleep(1)
        word = q.get()
        print("Starting " + word +  " with queue length " + str(q.qsize()))
        # Time
        currentTime = datetime.now()
        appIDList = ""
        first = 0
        payload = {'wd': word, 
                    'data_type':'app'}
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        baseUrl = 'https://shouji.baidu.com'

        #Fetching URLs of apps returned in search query
        r1 = requests.get(baseUrl + '/s', params=payload, headers=headers)
        #soup_page = BeautifulSoup(r1.content,'html.parser',from_encoding='utf-8')
        soup_page = BeautifulSoup(r1.content.decode('utf-8','ignore'),'html.parser')
        soup_apps = soup_page.find_all('div',class_='app')
        if len(soup_apps) == 0:
            continue
        appUrls = []
        for app in soup_apps:
            appUrls.append(baseUrl + app.find('a')['href'])
        try:
            #Parsing details for each returned app by visiting link
            for detailsUrl in appUrls:
                r2 = requests.get(detailsUrl,headers=headers)
                #soup = BeautifulSoup(r2.text,'lxml')
                soup = BeautifulSoup(r2.content.decode('utf-8','ignore'),'html.parser')
                downloads = soup.find('div',class_='detail').find('span',class_='download-num').get_text().split(':')[1][1:-3]
                desc = soup.find('div', class_='brief-long').get_text()
                print(desc)
                details = soup.find('div',class_='area-one-setup').find('span')
                downloadUrl = details['data_url']
                appID = details['data_package']
                # if appID in df.appID.values: //Parsing out duplicates to save time, using a pandas dataframe consisting of all currently aggregated data
                #     print("Existing app found: " + appID)
                #     continue
                # else:
                #   print("New app found: " + appID)
                title = details['data_name']
                print(title)
                version = details['data_versionname']
                fileSize = details['data_size']

                insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, appName=title,
                        desc=desc,file_size=fileSize, downloadCount = downloads, downloadURL=downloadUrl,
                                websiteName='shouji.baidu.com', createdAt=currentTime,version=version))
        except Exception as e:
            print("Error getting results for the query " + word)
            continue
        if first != 0:
            appIDList = appIDList + ","
        appIDList = appIDList + appID
        first = 1
        # Create appIdTable & suggestionTable in DB
        appIdTable = getTable(db, 'AppId')    

        # Create entries for tables
        currentTime = datetime.now()
        appIdTableEntry = (word, appIDList, 'shouji.baidu.com', currentTime)

        # Enter into appIdTable
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'shouji.baidu.com', createdAt = currentTime))

def xiaomi(db,q):
    print("Crawling Xiaomi App Store")
    # Create appDetailsTable in DB
    appDetailsTable = getTable(db, 'AppDetails')
    while(q.empty() != True):
        try:
            time.sleep(1)
            word = q.get()
            print("Starting " + word +  " with queue length " + str(q.qsize()))
            # Time
            currentTime = datetime.now()
            appIDList = ""
            first = 0
            payload = {'keywords': word}
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            baseUrl = 'https://app.mi.com'

            #Fetching URLs of apps returned in search query
            try:
                r1 = requests.get(baseUrl + '/search', params=payload, headers=headers,timeout=10)
            except Exception as e:
                continue
            soup_page = BeautifulSoup(r1.text,'html.parser')
            soup_page2 = soup_page.find_all('div',class_='applist-wrap')
            appUrls = []
            for link in soup_page2[0].find_all('a'):
                if 'details' in link.get('href'):
                    appUrls.append(baseUrl + link.get('href'))

            #Removing duplicates from list
            appUrls = list(dict.fromkeys(appUrls))

            #Parsing details for each returned app by visiting link
            for detailsUrl in appUrls:
                r2 = requests.get(detailsUrl,headers=headers)
                soup = BeautifulSoup(r2.text,'html.parser')
                title = soup.find('div',class_='app-info').h3.get_text().strip()
                desc = soup.find('div',class_='app-text').get_text()
                soup_details = soup.find_all('div',class_='float-left')
                appId = soup_details[3].find_all('div')[1].get_text().strip()
                fileSize = soup_details[0].find_all('div')[1].get_text().strip()
                version = soup_details[1].find_all('div')[1].get_text().strip()
                releaseDate = soup_details[2].find_all('div')[1].get_text().strip()

                insertIntoAppDetailsTable(appDetailsTable, dict(appID=appId, appName=title,
                        desc=desc,file_size=fileSize, publishDate = releaseDate,
                                websiteName='appgallery.mi.com', createdAt=currentTime,version=version))

                if first != 0:
                    appIDList = appIDList + ","
                appIDList = appIDList + appId
                first = 1
            # Create appIdTable & suggestionTable in DB
            appIdTable = getTable(db, 'AppId')    

            # Create entries for tables
            currentTime = datetime.now()
            appIdTableEntry = (word, appIDList, 'app.mi.com', currentTime)

            # Enter into appIdTable
            insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'app.mi.com', createdAt = currentTime))
        except requests.exceptions.ConnectionError:
            print("Timeout Error. Skipping word " + word)
            continue

def huawei(db,q):
    print("Crawling Huawei AppGallery App Store")
    # Create appDetailsTable in DB
    appDetailsTable = getTable(db, 'AppDetails')
    while(q.empty() != True):
        time.sleep(1)
        word = q.get()
        print("Starting " + word +  " with queue length " + str(q.qsize()))
        # Time
        currentTime = datetime.now()
        appIDList = ""
        first = 0

        payload = {'method':'internal.getTabDetail',
                    'serviceType':20,
                    'reqPageNum':1,
                    'uri':'searchApp|' + word,
                    'maxResults' : 25,
                    'version':'10.0.0',
                    'zone':'',
                    'locale':'cn'
                    }

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        url = 'https://web-dra.hispace.dbankcloud.cn/uowap/index'
        r = requests.get(url, params=payload, headers=headers)
        response = r.json()
        currentTime = datetime.now()
        # Create appDetailsTable in DB
        appDetailsTable = getTable(db, 'AppDetails')
        appIDList = ""
        first = 0
        try:
            for entry in response["layoutData"]:
                for item in entry["dataList"]:
                    appID = item["package"]
                # if appId in df.appID.values:  //Parsing out duplicates to save time, using a pandas dataframe consisting of all currently aggregated data. 
                # //To use this code, add the dataframe as an argument to the function definition and the function call
                #     print("Existing app found: " + appId)
                #     continue
                # else:
                #     print("New app found: " + appId)
                    fileSize = item["fullSize"]
                    apkHash = item["sha256"]
                    title = item["name"]
                    desc = item["memo"]
                    version = item["appVersionName"]
                    category = version = item["kindName"]
                    averageRating = item["stars"]
                    imageLink = item["icon"]
                    #savedetailsinDB
                    insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, appName=title,
                        desc=desc,file_size=fileSize,rating=averageRating,category=category,
                            filehash=apkHash, imageSource=imageLink,
                                websiteName='appgallery.huawei.com', createdAt=currentTime,version=version,other=str(item)))
        except TypeError:
            print("All results returned for query!")
            break
        if first != 0:
            appIDList = appIDList + ","
        appIDList = appIDList + appID
        first = 1
        # Create appIdTable & suggestionTable in DB
        appIdTable = getTable(db, 'AppId')    

        # Create entries for tables
        currentTime = datetime.now()

        # Enter into appIdTable
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'appgallery.huawei.com', createdAt = currentTime))


#Failing. Use user-agent fix
def apksupport(db, q):
    print("Starting apksupport")
    numberOfTerms = 0
    while(q.empty() != True):
        time.sleep(1)
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        payload = {'q': word, 't': 'app'}
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        r = requests.get('https://apk.support/search', params=payload, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        # Get App Names
        names_table = soup.find_all("div", attrs={"class": "it_column"})
        # Time
        currentTime = datetime.now()
        if(len(names_table) == 0):
            continue
        appIDList = ""
        first = 0
        # Create appDetailsTable in DB
        appDetailsTable = getTable(db, 'AppDetails')
        for name in names_table:
            # Developer Information
            developerPart = name.find_all("div", attrs={"class": "ss_tg"})
            developerPart = developerPart[0].find_all("a")
            developerTag = developerPart[0]['href']
            developerTag = developerTag[10:]
            developerName = developerPart[0].get_text()
            information = name.find_all("a")
            # Title
            titleTag = information[0].find_all("h3")
            title = titleTag[0].get_text()
            # Description
            descriptionTag = information[0].find_all("p")
            description = descriptionTag[0].get_text()
            # Stars
            starsTag = information[0].find_all("div", attrs = {"class" : "stars"})
            starsSpan = starsTag[0].find_all("span")
            stars = starsSpan[0]['title']
            starCount = stars[stars.rindex(' ')+1:]
            # AppID
            appID = information[0]['href']
            appID = appID[4 : ]
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            # Image Source Link
            imageTag = information[0].find_all("div", attrs={"class" : "seo_img"})
            imageTag = imageTag[0].find_all("img")
            imageSource = imageTag[0]['data-original']

            # Insert Into AppDetails Table (one per app)
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, description=description, stars=stars, imageSource=imageSource, developerName=developerName, websiteName='apk.support', createdAt=currentTime))

        # Suggestion Addition
        suggestionList = soup.find_all("div", attrs={"class": "suggest"})
        suggestionList = suggestionList[0].find_all("li")
        suggestions = []
        suggestionsString = ""
        i = 0
        for suggestion in suggestionList:
            suggestionName = suggestion.get_text()
            if (i != 0):
                suggestionsString = suggestionsString + ","
            suggestionsString = suggestionsString + suggestionName
            i = 1
            suggestions.append(suggestionName)
            modifiedSuggestionName = commaSeparated(suggestionName)
            if(modifiedSuggestionName not in wordSet):
                wordSet.add(modifiedSuggestionName)
                q.put(modifiedSuggestionName)

        # Create appIdTable & suggestionTable in DB
        appIdTable = getTable(db, 'AppId')
        suggestionTable = getTable(db, 'AppSuggestions')        

        # Create entries for tables
        currentTime = datetime.now()
        appIdTableEntry = (word, appIDList, 'apk.support', currentTime)
        suggestionTableEntry = (word, suggestionsString, 'apk.support', currentTime)


        # Enter into appIdTable & suggestionTable (one per word)
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apk.support', createdAt = currentTime))
        insertIntoSugesstionsTable(suggestionTable, dict(word=word, relatedSearchTerms= suggestionsString, websiteName = 'apk.support', createdAt = currentTime))

        numberOfTerms = numberOfTerms + 1
        if(numberOfTerms == 5000):
            break

#Completed
def apkdl(db, q):
    print("Starting apkdl")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        time.sleep(1)
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        payload = {'q': word}
        payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
        r = requests.get('https://apk-dl.com/search', params=payload_str)
        soup = BeautifulSoup(r.text, 'html.parser')
        names_table = soup.find_all("div", attrs={"class": "card no-rationale square-cover apps small"})
        if(len(names_table) == 0):
            continue
        appIDList = ""
        first = 0
        # Create appDetailsTable in DB
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        for name in names_table:
            appIDPart = name.find("a", attrs={"class": "card-click-target"})
            appID = appIDPart['href']
            imageLinkPart = name.find("img", attrs={"class": "cover-image lazy"})
            imageLink = imageLinkPart['data-original']
            titlePart = name.find("a", attrs={"class": "title"})
            title = titlePart.get_text()
            developerNamePart = name.find("a", attrs={"class": "subtitle"})
            developerName = developerNamePart.get_text()
            starsPart = name.find("div", attrs={"class" : "current-rating"})
            stars = starsPart['style']
            stars = stars.rsplit(' ', 1)[1]
            stars = stars[:-2]
            stars = int(stars)/10
            
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1

            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, stars=stars, imageSource=imageLink, developerName=developerName, websiteName='apk-dl.com', createdAt=currentTime))
            print('App Entered')
        
        appIdTable = getTable(db, 'AppId')
        appMainTableEntry = (word, appIDList, 'null', 'apk-dl.com')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apk-dl.com', createdAt = currentTime))
        numberOfTerms = numberOfTerms + 1
        if(numberOfTerms == 5000):
            break

#Completed
def apkpure(db, q):
    numberOfTerms = 0
    print("Starting apkpure")
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        checkMore = 1
        firstCheck = 1
        interval = 0
        appIDList = ""
        first = 0
        # Create appDetailsTable in DB
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        while(checkMore):
            if(firstCheck == 0):
                interval = interval + 15
            payload = {'q': word,  't': 'app', 'begin': interval}
            payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
            r = requests.get('https://apkpure.com/search-page', params=payload_str)
            soup = BeautifulSoup(r.text, 'html.parser')
            names_table = soup.find_all("dl", attrs={"class": "search-dl"})
            if(len(names_table) == 0):
                print("Skipping " + word)
                break
            for name in names_table:
                dtPart = name.find_all("dt")
                ddPart = name.find_all("dd")
                aPart = dtPart[0].find_all("a")
                appID = aPart[0]['href']
                imagePart = aPart[0].find_all("img")
                imageLink = imagePart[0]['src']
                titlePart = ddPart[0].find_all("p", attrs={"class": "search-title"})
                title = titlePart[0].find("a").get_text()
                starsPart = ddPart[0].find_all("span", attrs={"class": "score"})
                stars = starsPart[0]['title']
                stars = stars.rsplit(' ', 1)[1]
                pParts = ddPart[0].find_all("p")
                developerPart = pParts[1].find_all("a")
                developerName = developerPart[0].get_text()
                if first != 0:
                    appIDList = appIDList + ","
                appIDList = appIDList + appID
                first = 1
                
                insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, stars=stars, imageSource=imageLink, developerName=developerName, websiteName='apkpure.com', createdAt=currentTime))
            firstCheck = 0
            if(len(names_table) == 0):
                checkMore = 0
        appMainTableEntry = (word, appIDList, 'null', 'apkpure.com')
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apkpure.com', createdAt = currentTime))
        numberOfTerms = numberOfTerms + 1
        if(numberOfTerms == 5000):
            break

# Completed
def apkplz(db, q):
    print("Starting apkplz")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        payload = {'q': word}
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        r = requests.get('https://apkplz.net/search?', params=payload)
        soup = BeautifulSoup(r.text, 'html.parser')
        appList = soup.find_all("div", attrs={"class":"section row nop-sm"})
        appList = appList[0].find_all("div",attrs={"class":"row itemapp"})
        finalList = []
        appIDList = ""
        first = 0
        for app in appList:
            appDetails  = app.find_all("div",attrs={"class" : "col-md-12 col-sm-9 vcenter apptitle"}) 
            title = appDetails[0].find_all("a")
            title = title[0]['title']
            imageSource = app.find_all("div",attrs={"class" : "col-md-12 col-sm-3 vcenter"}) 
            imageSource = imageSource[0].find_all("img")
            imageSource = imageSource[0]["data-original"]
            appID = app.find_all("div",attrs={"class" : "col-md-12 col-sm-3 vcenter"}) 
            appID = appID[0].find_all("a")
            appID = appID[0]["href"]
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            
            # Insert Into App Table
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, websiteName='apkplz.com', createdAt=currentTime))
            
        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apkplz.com', createdAt = currentTime))

# Completed    
def apktada(db, q):
    print("Starting apktada")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        # word = 'apps+to+find+cheaters'
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        payload = {'q': word}
        r = requests.get('https://apktada.com/search?', params=payload)
        soup = BeautifulSoup(r.text, 'html.parser')
        appList = soup.find_all("div", attrs={"class":"section row nop-sm"})
        appList = appList[0].find_all("div",attrs={"class":"row itemapp"})
        appIDList = ""
        appDetailsTable = getTable(db, 'AppDetails')
        if len(appList) == 0:
            print('GO TO GOOGLE')
            appIDList = googleQueryParser(appDetailsTable, 'apktada.com', word)
        else:
            finalList = []
            first = 0
            currentTime = datetime.now()
            for app in appList:
                appDetails  = app.find_all("div",attrs={"class" : "col-md-12 col-sm-9 vcenter apptitle"}) 
                title = appDetails[0].find_all("a")
                title = title[0]['title']
                imageSource = app.find_all("div",attrs={"class" : "col-md-12 col-sm-3 vcenter"}) 
                imageSource = imageSource[0].find_all("img")
                imageSource = imageSource[0]["data-original"]
                appID = app.find_all("div",attrs={"class" : "col-md-12 col-sm-3 vcenter"}) 
                appID = appID[0].find_all("a")
                appID = appID[0]["href"]
                if first != 0:
                    appIDList = appIDList + ","
                appIDList = appIDList + appID
                first = 1
                
                # Insert Into App Table
                insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, websiteName='apktada.com', createdAt=currentTime))

        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        currentTime = datetime.now()
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apktada.com', createdAt = currentTime))
        # break

# Completed
def allfreeapk(db, q):
    print("Starting allfreeapk")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        payload = {'q': word}
        r = requests.get('https://m.allfreeapk.com/search.html?', params=payload)
        soup = BeautifulSoup(r.text, 'html.parser')
        appList = soup.find_all("div", attrs={"class":"list"})
        appList = appList[0].find_all("li")
        finalList = []
        appIDList = ""
        first = 0
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        for app in appList:
            appDetails  = app.find_all("div",attrs={"class":"l"}) 
            title =  app.find_all("div",attrs={"class":"r"}) 
            title = title[0].find_all("a")
            title = title[0].get_text()
            imageSource = appDetails[0].find_all("img")
            imageSource = imageSource[0]["data-original"]
            appID = appDetails[0].find_all("a")
            appID = appID[0]["href"]
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            
            # Insert Into App Table
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, websiteName='m.allfreeapk.com', createdAt=currentTime))

        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'm.allfreeapk.com', createdAt = currentTime))

# Completed
def apkfab(db, q):
    print("Starting apkfab")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        payload = {'q': word}
        r = requests.get('https://apkfab.com/search', params=payload)
        soup = BeautifulSoup(r.text, 'html.parser')
        appList = soup.find_all("div", attrs={"class":'list'})
        finalList = []
        appIDList = ""
        first = 0
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        for app in appList:
            title = app.find_all("div",attrs={"class":"title"})
            if(len(title)  < 1):
                continue
            title = title[0].get_text()
            rating = app.find_all("span", attrs={"class":"rating"})
            starCount = rating[0].get_text()
            imageSource = app.find_all("img")
            imageSource  = imageSource[0]['data-src']
            appID = app.find_all("a")
            appID = appID[0]['href']
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            
            # Insert Into App Table
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, stars= starCount, imageSource=imageSource, websiteName='apkfab.com', createdAt=currentTime))
            
        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apkfab.com', createdAt = currentTime))

# Completed
def malavida(db, q):
    print("Starting malavida")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        word = word.replace('+','-')
        r = requests.get('https://www.malavida.com/en/s/'+word)
        soup = BeautifulSoup(r.text, 'html.parser')
        appDetails = soup.find_all("section", attrs={"class":'app-list'})
        appList = soup.find_all("section", attrs={"class":'app-download'})
        counter = 0
        finalList = []
        appIDList = ""
        first = 0
        notFound = soup.find_all("section", attrs={"class":'not-found'})
        if(len(notFound)>0):
            continue
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        for app in appList:
            appSrc = app.find_all("div", attrs={"class":"title"})
            appDesc = app.find_all("p")
            imageSource = app.find_all('img')
            imageSource = imageSource[0]['src']
            appLink = appSrc[0].find_all("a")
            appID = appLink[0]['href']
            title = appLink[0].get_text()
            description = appDesc[0].get_text()
            counter = counter+1
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            
            # Insert Into App Table
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, description= description, websiteName='malavida.com', createdAt=currentTime))
            counter=counter+1

        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'malavida.com', createdAt = currentTime))

# Completed
def apkgk(db, q):
    print("Starting apkgk")
    numberOfTerms = 0
    while(q.empty() != True):
        word = q.get()
        print("Starting " + word + " " + str(numberOfTerms) + " with queue length " + str(q.qsize()))
        time.sleep(1)
        payload = {'keyword': word}
        r = requests.get('https://apkgk.com/search/', params=payload)
        soup = BeautifulSoup(r.text, 'html.parser')
        appId = soup.find_all("ul", attrs={"class":'topic-wrap'})
        if(len(appId) == 0):
            print("Skipping " + word)
            continue
        appId = appId[0].find_all("a")
        names_table = soup.find_all('div', attrs={"class":'topic-bg'})
        if(len(names_table) == 0):
            print("Skipping " + word)
            continue
        finalList = []
        appIDList = ""
        first = 0
        counter = 0
        appDetailsTable = getTable(db, 'AppDetails')
        currentTime = datetime.now()
        for name in names_table:
            appName = name.find_all("div", attrs={"class": "topic-tip-name"})
            appDesc = name.find_all("div", attrs={"class": "topic-tip-description"})
            appSrcMain  = name.find_all("div", attrs={"class": "c-lz-load"})
            imageTag = appSrcMain[0].find_all("img")
            imageSource = imageTag[0]['data-src']
            
            appID = appId[counter]['href']
            if first != 0:
                appIDList = appIDList + ","
            appIDList = appIDList + appID
            first = 1
            if (len(appDesc) > 0):
                description = appDesc[0].get_text()
            else:
                description = "NULL"
            title = appName[0].get_text()
            
            # Insert Into App Table
            insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, description= description, websiteName='apkgk.com', createdAt=currentTime))
            counter=counter+1

        #Insert Into Main Table
        appIdTable = getTable(db, 'AppId')
        insertIntoAppIdTable(appIdTable, dict(word=word, appIdList = appIDList, websiteName = 'apkgk.com', createdAt = currentTime))

def googleQueryParser(appDetailsTable, websiteName, word):
    print("Inside google search with website name as " + websiteName)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        'referer': 'https://www.google.com/',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'pragma': 'no-cache',
    }
    googleWord = "site:" + websiteName + '+' + word
    url = f"https://www.google.com/search?&q={googleWord}&sourceid=chrome&ie=UTF-8"
    time.sleep(2)
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    # print(soup)
    searchResults = soup.find_all('div', attrs={"class":'rc'})
    appIDList = ""
    first = 0
    for result in searchResults:
        searchURL = result.find('a')['href']
        if searchURL.find(websiteName) != -1 and searchURL.find('/app/') != -1:
            time.sleep(1)
            innerR = requests.get(searchURL)
            siteSoup = BeautifulSoup(innerR.text, 'html.parser')
            icon = siteSoup.find('img', attrs={"class":'section media'})
            if hasattr(icon, 'src'):
                imageSource = icon['src']
                title = siteSoup.find('h1').get_text()
                appID = ''
                stars = ''
                supplementaryData = ''
                otherData = siteSoup.find('ul', attrs={"class":'list-unstyled'})
                for data in otherData:
                    if type(data) is not bs4.element.NavigableString:
                        attributeName, value = extractForApkTadaWebPageViaGoogle(data.get_text())
                        if attributeName == 'Package Name':
                            appID = value
                        elif attributeName == 'Stars':
                            stars = value
                        else:
                            supplementaryData = supplementaryData + attributeName + ': ' + value + ', '
                supplementaryData = supplementaryData[:-2]
                if first != 0:
                    appIDList = appIDList + ","
                appIDList = appIDList + appID
                first = 1
                print('Extracted App')  
                currentTime = datetime.now()
                insertIntoAppDetailsTable(appDetailsTable, dict(appID=appID, title=title, imageSource=imageSource, websiteName='apktada.com', referrer='google.com', createdAt=currentTime, stars=stars, otherData=supplementaryData))
    print('FROM GOOGLE ' + str(len(appIDList)))
    return appIDList

if __name__=='__main__':
    chineseTermsQueue = readTermsAndCreateQueue('cn')
    db = databaseStartUp('sqlite:///database.db')