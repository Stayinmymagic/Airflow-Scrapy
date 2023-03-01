# Airflow-Scrapy

### 自動化爬蟲流程

![image](https://github.com/Stayinmymagic/Airflow-Scrapy/blob/master/data%20pipeline.png)

### 步驟解析

1. 爬取代理伺服器網站的免費ip，在爬新聞網時可以用代理ip隱藏真實ip位置。
   https://www.us-proxy.org/
2. 確認代理ip列表是否有超過五個以上的ip，確保爬取美國新聞網時有多個ip可替換。
3. 若有通過上一個流程則進行爬取美國新聞網，沒有的話則經過一段時間回到第一個任務重新爬取代理ip。
4. 爬完新聞後存入MySQL中。

