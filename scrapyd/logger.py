import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from twisted.web import server, resource
from twisted.web.static import File
from scrapyd.webservice import WsResource
from .utils import native_stringify_dict
from copy import copy

from json import loads
from codecs import open as c_open
from re import findall
from .utils import get_spider_list,get_spider_queues,JsonResource
from .config import Config

def send_event(request , string ,event="message"):
    request.setHeader(b'content-type', b'text/event-stream; charset=utf-8')
    request.setHeader('Access-Control-Allow-Origin',"*")
    request.setHeader('Access-Control-Expose-Headers',"*")
    request.setHeader('Access-Control-Allow-Credentials',"true")
    request.setHeader("Connection", "keep-alive")
    request.write( str.encode("event: %s\n"%event) )
    message = "data: "+string + "\n\n"
    request.write( str.encode(message) )

class CustomLog:
    content = list()
    def __init__(self,*args):
        self.logsdir,self.project,self.spider,self.job = args
        self.dir = os.path.join(os.getcwd(), self.logsdir,self.project,self.spider)
        self.path = os.path.join(os.getcwd(), self.logsdir,self.project,self.spider,self.job+".log")
        self.position = 0
        if os.path.isfile(self.path):
            self.file = self.open()
            self.content = self.file.readlines()
            self.position = self.file.tell()

        else:
            self.file = None

    def open(self):
        return c_open(self.path,"r","utf-8")
        

    @property
    def finished(self):
        return os.path.isfile(self.path) and len(self.content) > 0 and  "INFO: Spider closed" in self.content[-1] 
    
    @property
    def running(self):
        return os.path.isfile(self.path) and not self.finished
    
    @property
    def pending(self):
        qu = get_spider_queues(Config())[self.project].list()
        return self.job in [a["_job"] for a in qu] 

    @property
    def raw_contant(self):
        return "\n".join(self.content)

    def update(self):
        if self.file is None:
            self.file = self.open()
        self.file.seek(self.position)
        new_content = self.file.readlines()
        self.content += new_content
        self.position = self.file.tell()
        return new_content

    def parse_stat(self):
        regex = r"INFO: Dumping Scrapy stats:(?:[\s\S]*){(?:[\s\S]*)}"
        stat_text =  findall(regex,self.raw_contant)[0]
        stat_text = stat_text[stat_text.find("{"):]
        for date in findall(r"datetime.datetime\((?:\d*, ){6}\d*\)",stat_text):
            date_text = date.replace("datetime.datetime","").replace("(",'"').replace(")",'"')
            stat_text = stat_text.replace(date,date_text)
        return loads(stat_text.replace("'",'"')) 



class JobStatus(WsResource):
    def render_GET(self, txrequest):
        args = native_stringify_dict(copy(txrequest.args), keys_only=False)
        if "spider" not in args or "project" not in args or "job" not in args:
            return self.error("bad request")
        job = args.get('job')[0]
        project = args.get('project')[0]
        spider = args.get('spider')[0]
        log = CustomLog("logs",project,spider,job)
        if log.finished:
            return {'status':"finished",**log.parse_stat()}
        elif log.pending:
            return {"status":"pending"}
        else:
            return {"status":"running"}
    def error(self,message):
        return {"status": "error", "message": message}

class LogProducer:
    def __init__(self,request,log):
        self.request = request
        self.log = log
        if not self.log.finished:
            ss = FileSystemEventHandler()
            ss.on_modified = self.on_modified
            self.observer = Observer()
            self.observer.schedule(ss, log.dir, recursive=True)
            self.observer.start()
    
    def send_read(self,content):
        for li in content:
            send_event(self.request,li)
        if self.log.finished:
            self.request.finish()

    def on_modified(self,e):
        if self.log.job in e.src_path:
            self.send_read(self.log.update())
    def stopProducing(self):
        pass
    
    def pauseProducing(self):
        pass
    def resumeProducing(self):
        pass

class LiveLogger(resource.Resource):
    def render_GET(self, request):
        args = native_stringify_dict(copy(request.args), keys_only=False)
        if "job" not in args or "project" not in args or "spider" not in args:
            self.send_error(request,"bad request")
        
        project = args.pop('project')[0]
        spider = args.pop('spider')[0]
        job = args.pop('job')[0]
        logss = CustomLog("logs",project,spider,job)

        can_log = logss.pending or logss.running or logss.finished
        
        if not can_log:
            self.send_error(request,"job isnt relavant to %s"%spider)
        else:
            producer = LogProducer(request,logss)
            producer.send_read(logss.content)
            if not logss.finished:
                request.registerProducer(producer,1)
    
        return server.NOT_DONE_YET 
     
    def send_error(self,request,message):
        send_event(request,str(message),event= "error")
        request.finish()

class staticLogger(File):
    def render_GET(self, request):
        request.setHeader('Access-Control-Allow-Origin',"*")
        request.setHeader('Access-Control-Expose-Headers',"*")
        request.setHeader('Access-Control-Allow-Credentials',"true")
        return super(staticLogger,self).render_GET(request)