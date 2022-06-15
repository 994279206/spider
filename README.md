### 基于 scrapy-redis 分布式爬虫框架（项目还没完成，持续更新中）

本分布式爬虫分为主爬虫和子爬虫,主爬虫主要负责详情队列的解析以及判重翻页功能,子爬虫负责详情数据的采集功能   
#### 导入环境依赖
python 3.9 
```angular2html
pip install -r requirements.txt
pip install -r requirements.txt -i https://pypi.org/simple （国内源）
```
父爬虫启动方式 scrapy crawl spider   
子爬虫启动方式 scrapy crawl spider -a master=0




