from multiprocessing import Process
from threading import Thread

from flask import Blueprint, Flask
from time import sleep


class StreamingApp(Blueprint):
    """
    A streaming app using omegaml as its data storage

    This is a simple Flask app that runs a streaming processing function,
    receiving and processing data written to a given minibatch stream. Optionally
    the app can have its own UI.

    Syntax:
        app = StreamingApp()
        app.start_streaming(consumer, on=om)
        app.run()

        Where consumer is a function that creates a @streaming processor, i.e.

        def consumer(url=None):
            @streaming('stream-name', url=url)
            def processing(window):
                ...

        Notes:
        - the consumer(url=) argument receives the omegaml's data store url. If
          not given it defaults to env $MINIBATCH_MONGO_URL, reverting to $MONGO_URL.
          Typically specify as om.datasets.mongo_url
        - the start_streaming(..., on=om) argument receives the omegaml instance.
          this is optional, if not given, import omegaml as om is used to create
          an instance from the environment

    Usage:
        A working example

        # in your app.py
        def create_app(server=None, uri=None, **kwargs):
            # this creates the flask app as the serverless app to run the stream
            app = StreamingApp(server=server, uri=uri)

            # add any route you like to have, if any
            @app.route('/')
            def index():
                import omegaml as om
                return '{}'.format(om.datasets.get('test-stream'))

            # start it up
            app.server.register_blueprint(app)
            app.start_streaming(consumer)
            return app

        def consumer(url=None):
            # this creates the processing function attached to the 'test' stream
            # note consumer will be run on a sep process, processing will be run on a pool
            @streaming('test', url=url)
            def processing(window):
                import omegaml as om
                om.datasets.put(window.data, 'test-stream', raw=True)

        # to test locally
        if __name__ == '__main__':
            app = create_app(server=True)
            app.run()
            app.stop()

        # in some other process send data to the stream
        $ python
        [] import minibatch as mb
           import omegaml as om

           stream = mb.stream('test', url=om.defaults.OMEGA_MONGO_URL)
           stream.append(dict(data='foo'))

        # by refreshing on http://localhost:5000 you will see the data written to the stream
    """

    def __init__(self, consumer=None, name=None, import_name=None, uri=None, server=None, **kwargs):
        name = name or 'streaming'
        import_name = import_name or __name__
        super().__init__(name, import_name, url_prefix=uri, **kwargs)
        self.setup(server, import_name, consumer)

    def setup(self, server, import_name, consumer):
        if server is None or server is True:
            server = Flask(import_name)
        self.server = server
        self.consumer = consumer
        self.proc = None
        self.register_routes(self)

    def register_routes(self, app):
        @app.route('/status')
        def streaming_app_status():
            return "running minibatch"

    def _start_consumer(self, consumer=None, on=None, debug=False):
        from minibatch import authenticated_url
        import omegaml as om

        consumer = consumer or self.consumer
        assert consumer is not None, "you must specify a consumer=function(url) to create the @streaming processor"

        om = on or om
        consumer_kwargs = dict(url=authenticated_url(om.defaults.OMEGA_MONGO_URL))
        if debug:
            consumer(**consumer_kwargs)
        elif self.proc is None:
            self.proc = Process(target=consumer,
                                kwargs=consumer_kwargs)
            self.proc.start()

        watcher = Thread(target=self.watcher)
        watcher.start()

    def watcher(self):
        while self.proc.is_alive():
            sleep(.1)
        print("WARNING streaming processor has stopped, exitcode {}".format(self.proc.exitcode))

    def _stop_consumer(self):
        if self.proc:
            self.proc.terminate()
            self.proc.join()

    def run_server(self, *args, **kwargs):
        return self.server.run(*args, **kwargs)

    # convenience
    run = run_server
    start = _start_consumer
    stop = _stop_consumer
    start_streaming = _start_consumer
    stop_streaming = _stop_consumer
