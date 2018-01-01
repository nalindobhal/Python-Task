import cherrypy
import os

from jinja2 import Environment, FileSystemLoader
import modules

# GET CURRENT DIRECTORY
CUR_DIR = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(CUR_DIR), trim_blocks=True)


class ZerodhaTask(object):
    @cherrypy.expose
    def index(self):
        template = env.get_template('site_base.html')
        csv_data = modules.fetch_data(1)
        return template.render(title="Get Today's Bhav" , return_type='index', data=csv_data)

    @cherrypy.expose
    def search(self, **kwargs):
        get_value = kwargs.get('search')
        if get_value is None:
            return
        template = env.get_template('search_page.html')
        value = modules.get_type(get_value)
        if type(value) == int:
            result = modules.search_data_by_code(value)
            ctx = {'status': 'ok', 'result': result}
        else:
            result = modules.search_data_by_name(value)
            ctx = {'status': 'ok', 'result': result}
        return template.render(value=value, return_type='search', data=ctx)

    @cherrypy.expose
    def sort_entries(self, **kwargs):
        get_value = kwargs['id']
        template = env.get_template('sorted_entries.html')
        value = modules.get_type(get_value)
        if type(value) == int:
            result = modules.fetch_data(value, reverse=False)
            return template.render(data=result)


if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './static'
        }
    }

    #Settings for deployment
    # cherrypy.server.socket_host = '0.0.0.0'
    # cherrypy.server.socket_port = 80

    #Run server
    cherrypy.quickstart(ZerodhaTask(), '/', conf)
