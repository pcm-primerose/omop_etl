import json
import datetime as dt


class ISOJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (dt.date, dt.datetime)):
            return o.isoformat()
        return super().default(o)
