from twisted.web.static import File

class staticLogger(File):
    def render_GET(self, request):
        request.setHeader('Access-Control-Allow-Origin', '*')
        request.setHeader('Access-Control-Allow-Methods', 'GET')
        request.setHeader('Access-Control-Allow-Headers', 'x-prototype-version,x-requested-with')
        request.setHeader('Access-Control-Max-Age', 2520) # 42 hours
        return super(staticLogger,self).render_GET(request)