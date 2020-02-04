from functools import partial
import base64

import dill


class FunctionMarshaller(object):
    """
    Serialize/deserialize any function or BatchCallable
    * serialize any function and its arguments for transport, called a job
    * deserialize a job back to a function and its arguments
    inspired by https://medium.com/@emlynoregan/serialising-all-the-functions-in-python-cd880a63b591 # noqa
    """

    def _serialize_anyfunc(self, func, args, kwargs):
        # create serialized functions
        sfunc = dill.dumps(func, protocol=2)
        kind = 'object'
        # serialize arguments
        sargs = dill.dumps(args, protocol=2)
        skwargs = dill.dumps(kwargs, protocol=2)
        # create transportable representation
        sfunc = base64.encodebytes(sfunc).decode('latin1')
        sargs = base64.encodebytes(sargs).decode('latin1')
        skwargs = base64.encodebytes(skwargs).decode('latin1')
        return kind, sfunc, sargs, skwargs

    def serialize(self, func, *args, **kwargs):
        """
        serialize  a function object into transportable format
        """
        if hasattr(func, 'items'):
            func.items = [self._serialize_anyfunc(
                ifunc, args, kwargs) for ifunc, args, kwargs in func.items]
            funcjob = self._serialize_anyfunc(func, None, None)
        else:
            funcjob = self._serialize_anyfunc(func, args, kwargs)
        return funcjob

    def _deserialize_anyfunc(self, kind, sfunc, sargs, skwargs):
        # unwrap
        sfunc = base64.decodebytes(sfunc.encode('latin1'))
        sargs = base64.decodebytes(sargs.encode('latin1'))
        skwargs = base64.decodebytes(skwargs.encode('latin1'))
        func = dill.loads(sfunc)
        args = dill.loads(sargs)
        kwargs = dill.loads(skwargs)
        return func, args, kwargs

    def deserialize(self, sjob):
        # decode serialized arguments to get back a function
        kind, sfunc, sargs, skwargs = sjob
        func, args, kwargs = self._deserialize_anyfunc(
            kind, sfunc, sargs, skwargs)
        # resolve functions
        if hasattr(func, 'items'):
            func.items = [
                self._deserialize_anyfunc(kind, sfunc, sargs, skwargs)
                for kind, sfunc, sargs, skwargs in func.items]
        if args:
            func = partial(func, *args)
        if kwargs:
            func = partial(func, **kwargs)
        return func


class SerializableFunction:
    # a function object that is fully serializable by pickle
    # and callable by a remote process
    def __init__(self, fn, *args, **kwargs):
        from minibatch.marshaller import FunctionMarshaller
        self.sjob = FunctionMarshaller().serialize(fn, *args, **kwargs)

    def __call__(self):
        from minibatch.marshaller import FunctionMarshaller

        func = FunctionMarshaller().deserialize(self.sjob)
        return func()


class MinibatchFuture:
    # a type-safe future with custom attributes
    # we could just as well store future._mb_data directly
    # but this is nicer code-wise
    def __init__(self, future, **data):
        self.future = future
        if hasattr(future, '_mb_data'):
            # we have seen this future before, reuse data dict
            self.data = future._mb_data
        else:
            # new future, add our data dict
            self.data = dict(data)
            self.future._mb_data = self.data

    def __getattr__(self, k):
        # if we know of the attribute as a key in our data, use it
        # else return the future's attribute
        if k in self.data:
            return self.data[k]
        return getattr(self.future, k)

    def __setitem__(self, k, v):
        # update our data
        self.data[k] = v
